require "db"
require "uuid"
require "active-model"
require "./database"
require "./changefeed"
require "./persistence"
require "./extensions"
require "./query"
require "./table"
require "./errors"
require "./validators/*"

module PgORM
  abstract class Base < ActiveModel::Model
    include ActiveModel::Validation
    include ActiveModel::Callbacks

    include Associations
    include Persistence
    extend Query
    include Table
    include Validators
    extend ChangeReceiver

    TABLES = [] of String

    macro inherited
      # macro level key => type
      PRIMARY_KEY_TYPES = {} of Nil => Nil
      PRIMARY_KEYS = [{:id}]

      private macro primary_key(key, *args)
        \{% PRIMARY_KEYS.clear %}
        \{% PRIMARY_KEYS << ({key.id.symbolize} + args.map(&.id.symbolize)) %}
      end

      def self.primary_key
        keys = PRIMARY_KEYS[0]
        keys.size == 1 ? keys[0] : keys
      end

      def primary_key
        PRIMARY_KEYS[0]
      end

      macro finished
        {% unless @type.abstract? %}
        __process_table__
        {% end %}
      end
    end

    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    @extra_attributes : Hash(String, ::PgORM::Value)?

    protected def extra_attributes=(@extra_attributes : Hash(String, Value))
    end

    def extra_attributes : Hash(String, ::PgORM::Value)
      @extra_attributes ||= {} of String => ::PgORM::Value
    end

    macro __customize_orm__
      {% if HAS_KEYS[0] && !PERSIST.empty? %}
          check_converters_if_any
          __set_primary_key__
          __create_db_initializer__
      {% end %}
    end

    def_equals attributes, changed_attributes

    macro default_primary_key(name, auto_generated = true, converter = nil, **tags)
      primary_key {{name.var.id.symbolize}}
      attribute {{name.var}} : {{name.type.resolve}}, mass_assignment: false, auto_generated: {{auto_generated}},
            converter: {{converter}}, es_type: "keyword" {% unless tags.empty? %}, {{**tags}} {% end %}
    end

    private macro __set_primary_key__
      {% for name, opts in PERSIST %}
        {% if PRIMARY_KEYS[0].includes?(name.symbolize) %}
          {% PRIMARY_KEY_TYPES[name.symbolize] = opts[:klass] %}

          {% if !opts[:klass].nilable? && (name.symbolize == :id && opts[:auto_generated] != false) %}
            {% opts[:auto_generated] = true %}
          {% end %}
        {% end %}
      {% end %}

      {% if PRIMARY_KEY_TYPES.empty? %}
        {% raise "primary keys in #{@type.id} must be defined as an attribute: #{PRIMARY_KEYS[0]}" %}
      {% end %}

      {% if PRIMARY_KEY_TYPES.size == 1 %}
        alias PrimaryKeyType = {{ PRIMARY_KEY_TYPES.values[0] }}
      {% else %}
        alias PrimaryKeyType = Tuple({{ PRIMARY_KEY_TYPES.values.map(&.id.stringify).join(", ").id }}) | Enumerable({{ PRIMARY_KEY_TYPES.values.map(&.id.stringify).join(" | ").id }})
      {% end %}

      # Always returns this record's primary key value, even when the primary key
      # isn't named `id`
      {% if !PRIMARY_KEYS[0].includes?(:id) %}
        def id
          {% if PRIMARY_KEY_TYPES.size == 1 %}
            self.{{ PRIMARY_KEYS[0][0].id }}
          {% else %}
            {
              {% for name in PRIMARY_KEYS[0] %}
                self.{{name.id}},
              {% end %}
            }
          {% end %}
        end
      {% end %}

      def id?
        {% if PRIMARY_KEY_TYPES.size == 1 %}
          self.attributes_tuple[:{{ PRIMARY_KEYS[0][0].id }}]?
        {% else %}
          attr = self.attributes_tuple
          {
            {% for name in PRIMARY_KEYS[0] %}
              attr[:{{name.id}}]?,
            {% end %}
          }
        {% end %}
      end

      def primary_key_hash
        attr = self.attributes_tuple
        {
          {% for name in PRIMARY_KEYS[0] %}
          {{name.id}}: attr[:{{name.id}}]?,
          {% end %}
        }
      end

      # We define set_primary_key_after_create for just the first primary key
      # as it's both less likely that composite primary keys will be auto generated
      # and this makes the code simpler for the more common path
      {% pk_name = PRIMARY_KEYS[0][0]
         pk_type = PRIMARY_KEY_TYPES[pk_name] %}

      {% if %w(Int8 Int16 Int32 Int64).includes?(pk_type.stringify) %}
        @[AlwaysInline]
        protected def set_primary_key_after_create(value : Int)
          self.{{pk_name.id}} = {{pk_type}}.new(value)
        end
      {% else %}
        @[AlwaysInline]
        protected def set_primary_key_after_create(value : {{pk_type}})
          self.{{pk_name.id}} = value
        end
      {% end %}

      @[AlwaysInline]
      protected def set_primary_key_after_create(value)
        raise "Expected primary key type : #{PrimaryKeyType}, received: #{value}, type: #{typeof(value)}"
      end
    end

    private macro __create_db_initializer__
      def self.from_rs(%rs : ::DB::ResultSet)
        %objs = Array(self).new
        %rs.each do
          %objs << self.new(%rs)
        end
        %objs
      ensure
        %rs.close
      end
      def self.new(rs : ::DB::ResultSet)
        %extra_attributes = nil
        {% for key, opts in PERSIST %}
           var_{{key}} = ActiveModel::Model::None.new
        {% end %}

        rs.each_column do |%column_name|
          case %column_name
            {% for key, opts in PERSIST %}
            when {{key.stringify}}
              var_{{key}} =
                {% if opts[:converter] %}
                  {{opts[:converter]}}.from_rs(rs)
                {% elsif opts[:klass].union_types.reject(&.==(Nil)).first < Array %}
                  rs.read({{opts[:klass]}})
                {% elsif opts[:klass] < Set %}
                  rs.read(Array({{opts[:klass].type_vars.join(' ').id}})).to_set
                {% elsif opts[:klass].union_types.reject(&.==(Nil)).first < Enum %}
                  {% if opts[:klass].nilable? %}
                     if (v = rs.read(Int32?))
                      {{opts[:klass].union_types.reject(&.==(Nil)).first}}.from_value(v)
                     else
                        nil
                     end
                  {% else %}
                  {{opts[:klass]}}.from_value(rs.read(Int32))
                  {% end %}
                {% elsif (::PgORM::Value).union_types.includes?(opts[:klass].union_types.reject(&.==(Nil)).first) == false %}
                   {{opts[:klass].union_types.reject(&.==(Nil)).first}}.from_json(JSON::Any.new(rs.read(JSON::PullParser{% if opts[:klass].nilable? %}?{% end %})).to_json)
                {% elsif opts[:klass] < Int %}
                  {{opts[:klass]}}.new(rs.read(Int))
                {% else %}
                  rs.read({{opts[:klass]}})
                {% end %}
            {% end %}
            else
              %extra_attributes ||= {} of String => ::PgORM::Value
              %extra_attributes[%column_name] = begin
              ext_val = rs.read

              case ext_val
              when JSON::PullParser then JSON::Any.new(ext_val).to_json
              #when Slice(UInt8) then String.new(ext_val)
              else
                  ext_val.as?(::PgORM::Value)
              end
            end
              #%extra_attributes[%column_name] = rs.read(PgORM::Value)
          end
        end

        %result = new(
          {% for key, opts in PERSIST %}
            {{key}}: var_{{key}},
          {% end %}
        )

        %result.extra_attributes = %extra_attributes if %extra_attributes
        %result.new_record = false
        %result.clear_changes_information
        %result
      end

      protected def load_attributes(rs : ::DB::ResultSet)
        {% begin %}
        rs.each_column do |%column_name|
            case %column_name
              {% for key, opts in PERSIST %}
              when {{key.stringify}}
                @{{key}} =
                  {% if opts[:converter] %}
                    {{opts[:converter]}}.from_rs(rs)
                  {% elsif opts[:klass].union_types.reject(&.==(Nil)).first < Array %}
                    rs.read({{opts[:klass]}})
                  {% elsif opts[:klass].union_types.reject(&.==(Nil)).first < Set %}
                  rs.read(Array({{opts[:klass].type_vars.join(' ').id}})).to_set
                  {% elsif opts[:klass].union_types.reject(&.==(Nil)).first < Enum %}
                    {% if opts[:klass].nilable? %}
                      if (v = rs.read(Int32?))
                        {{opts[:klass].union_types.reject(&.==(Nil)).first}}.from_value(v)
                      else
                          nil
                      end
                    {% else %}
                      {{opts[:klass]}}.from_value(rs.read(Int32))
                    {% end %}
                 {% elsif (::PgORM::Value).union_types.includes?(opts[:klass].union_types.reject(&.==(Nil)).first) == false %}
                    {{ opts[:klass].union_types.reject(&.==(Nil)).first }}.from_json(JSON::Any.new(rs.read(JSON::PullParser{% if opts[:klass].nilable? %}?{% end %})).to_json)
                 {% elsif opts[:klass] < Int %}
                    {{opts[:klass]}}.new(rs.read(Int))
                 {% else %}
                    rs.read({{opts[:klass]}})
                  {% end %}
              {% end %}
              else
                extra_attributes[%column_name] = rs.read(PgORM::Value) rescue nil
            end
        end
        {% end %}
      end

      # :nodoc:
      def [](attr : Symbol)
        persistent_attributes[attr]
      end

      # :nodoc:
      def []=(attr : Symbol, value)
        case attr
          {% for key, opts in FIELDS %}
        when {{key.symbolize}}
            {% ivar_type = opts[:klass] %}
            {% if !ivar_type.nilable? %}
              if primary_key.includes?({{key.symbolize}})
                @{{key}} = value.as?({{ivar_type}} | Nil)
              else
                @{{key}} = value.as?({{ivar_type}})
              end
            {% else %}
              @{{key}} = value.as?({{ivar_type}})
            {% end %}
          {% end %}
        else
          raise ::PgORM::Error.new("no such attribute: #{self.class.name}[:#{attr}]")
        end
      end

      def persistent_attributes
        {
          {% for name, opts in PERSIST %}
            {% if !opts[:tags] || (tags = opts[:tags]) && (!tags[:read_only]) %}
              {% if opts[:klass].union_types.reject(&.==(Nil)).first < Array && !opts[:converter] %}
              :{{name}} => PQ::Param.encode_array(@{{name}} || ([] of {{opts[:klass]}})),
              {% elsif opts[:klass].union_types.reject(&.==(Nil)).first < Set %}
              :{{name}} => PQ::Param.encode_array((@{{name}} || (Set({{opts[:klass]}}).new)).to_a),
              {% elsif opts[:klass].union_types.reject(&.==(Nil)).first < Enum && !opts[:converter] %}
              :{{name}} => @{{name}}.try &.value,
              {% elsif (::PgORM::Value).union_types.includes?(opts[:klass].union_types.reject(&.==(Nil)).first) %}
              :{{name}} => @{{name}},
              {% elsif opts[:converter] %}
                {% if opts[:converter] && opts[:converter].resolve.class.methods.map(&.name.id).includes?(:to_rs.id) %}
                  :{{name}} => {{opts[:converter]}}.to_rs(@{{name}}),
                {% else %}
                  :{{name}} => {{opts[:converter]}}.to_json(@{{name}}),
                {% end %}
              {% else %}
              :{{name}} => @{{name}}.to_json,
              {% end %}
            {% end %}
          {% end %}
        } {% if PERSIST.empty? %} of Nil => Nil {% end %}
      end

      def changed_persist_attributes
        all = persistent_attributes
        {% for name, index in PERSIST.keys %}
          all.delete(:{{name}}) unless @{{name}}_changed
        {% end %}
        all
      end

      # :nodoc:
      @@change_block : Array(ChangeFeed({{@type.id}})) = Array(ChangeFeed({{@type.id}})).new

      # Changefeed at row (if `id` passed) or whole table level.
      # Returns a `ChangeFeed` instance which can be used to invoke async callbacks via `on` or
      # use blocking `Iterator` via `each` method.
      def self.changes(
        {% if PRIMARY_KEYS[0].size == 1 %}
          id : {{ PRIMARY_KEY_TYPES[PRIMARY_KEYS[0][0]] }} | Nil = nil
        {% else %}
          id : Tuple(
            {% for key in PRIMARY_KEYS[0] %}
              {{ PRIMARY_KEY_TYPES[key] }},
            {% end %}
          ) | Nil = nil
        {% end %}
      ) : ChangeFeed
        feed = ChangeFeed({{@type.id}}).new(id, self)
        @@change_block << feed
        ::PgORM::Database.listen_change_feed(table_name, self) if @@change_block.size == 1
        feed
      end

      # :nodoc:
      def self.stop_changefeed(receiver : ChangeFeed)
        @@change_block.delete(receiver)
        ::PgORM::Database.stop_change_feed(table_name) if @@change_block.empty?
      end

      # :nodoc:
      def self.changefeed (event : ::PgORM::ChangeReceiver::Event, change : String, update : String? = nil)
        model = from_trusted_json(change)

        if col_update = update
          model_old_raw = JSON.parse(change).as_h
          col_changes = JSON.parse(col_update).as_a.map(&.as_h)
          cols = col_changes.map(&.["field"])

          # build the old model from the changes
          {% for key, opts in PERSIST %}
            if cols.includes?({{key.stringify}})
              delta = col_changes[cols.index!({{key.stringify}})]
              model_old_raw[{{key.stringify}}] = delta["old"]
            end
          {% end %}
          model_old = from_trusted_json(model_old_raw.to_json)

          # apply the changes to the model
          {% for key, opts in PERSIST %}
            if cols.includes?({{key.stringify}})
              model_old.{{key}} = model.{{key}}
            end
          {% end %}

          model = model_old
        end
        model.new_record = false
        model.destroyed = true if event.deleted?
        @@change_block.each {|cb| spawn{cb.on_event(event, model)}}
      end

      def self.on_error(err : Exception | IO::Error)
        @@change_block.each {|cb| cb.on_error(err)}
      end

      class ChangeFeed(T)

        # Represents a Changefeed Change, where `event` represents CRUD operation and value is the model
        record(Change(T),
          value : T,
          event : ::PgORM::ChangeReceiver::Event,
        ) do

          def created?
            event.created?
          end

          def updated?
            event.updated?
          end

          def deleted?
            event.deleted?
          end
        end

        include Iterator(Change(T))

        @callback : (Change(T) -> Nil)? = nil

        def initialize(
          {% if PRIMARY_KEYS[0].size == 1 %}
            @id : {{ PRIMARY_KEY_TYPES[PRIMARY_KEYS[0][0]] }} | Nil = nil,
          {% else %}
            @id : Tuple(
              {% for key in PRIMARY_KEYS[0] %}
                {{ PRIMARY_KEY_TYPES[key] }},
              {% end %}
            ) | Nil = nil,
          {% end %}
          @parent : T.class = T.class
        )
          @channel = Channel(Change(T)).new
        end

       # Method expects a block which will get invoked with `Change(T)` parameter on change events received from `EventBus`
       def on(&block : Change(T) -> Nil)
          @callback = block
       end

        def stop
          @channel.close
          @parent.stop_changefeed(self)
        end

        def next
          val = @channel.receive
          if val.nil?
            Iterator::Stop::INSTANCE
          else
            val.not_nil!
          end
        rescue Channel::ClosedError
          Iterator::Stop::INSTANCE
        end

        # :nodoc:
        def on_event(evt : ::PgORM::ChangeReceiver::Event, model : T)
          if (@id.nil? || @id == model.id)
            change = Change(T).new(model, evt)
            @callback.try &.call(change)
            # discard previous event (if any), so that we only keep copy of latest
            # received event.
            spawn do
              select
                when @channel.send(change)
                else @channel.receive?
              end rescue nil
            end
          end
        end

        # :nodoc:
        def on_error(err : Exception | IO::Error)
            stop rescue nil
            raise err
        end
      end
    end

    macro __nilability_validation__
      # Validate that all non-nillable fields have values.
      def validate_nilability
        {% if HAS_KEYS[0] && !PERSIST.empty? %}
          {% for name, opts in PERSIST %}
            {% if !opts[:tags] || (tags = opts[:tags]) && (!tags[:read_only]) %}
              {% if !opts[:klass].nilable? && !opts[:auto_generated] %}
                validation_error({{name.symbolize}}, "should not be nil" ) if @{{name.id}}.nil?
              {% end %}
            {% end %}
          {% end %}
        {% end %}
      end
    end

    private macro check_converters_if_any
      {% for name, opts in PERSIST %}
        {% if (converter = opts[:converter]) && (!converter.resolve.class.methods.map(&.name.id).includes?(:from_rs.id)) %}
          {% raise "Converter '#{converter.id}' provided for attribute #{name}, doesn't support method for parsing resultset" %}
        {% end %}
      {% end %}
    end

    def to_json(json : JSON::Builder)
      invoke_props
      super
    end
  end
end

require "./relation"

module PgORM
  module Associations
    # :nodoc:
    annotation SerializeMarker
    end

    # Declares a belongs to relationship.
    #
    # This will add the following methods:
    # - `association` returns the associated object (or nil);
    # - `association=` assigns the associated object, assigning the foreign key;
    # - `build_association` builds the associated object, assigning the foreign
    #   key if the parent record is persisted, or delaying it to when the new
    #   record is saved;
    # - `create_association` creates the associated object, assigning the foreign
    #   key, granted that validation passed on the associated object;
    # - `create_association!` same as `create_association` but raises a
    #   ::PgORM::Error::RecordNotSaved exception when validation fails;
    # - `reload_association` to reload the associated object.
    #
    # For example a Book class declares `belongs_to :author` which will add:
    #
    # - `Book#author` (similar to `Author.find(author_id)`)
    # - `Book#author=(author)` (similar to `book.author_id = author.id`)
    # - `Book#build_author` (similar to book.author = Author.new)
    # - `Book#create_author` (similar to book.author = Author.create)
    # - `Book#create_author!` (similar to book.author = Author.create!)
    # - `Book#reload_author` (force reload book.author)
    #
    # Options
    #
    # - `class_name` overrides the association class name (inferred as
    #   `name.camelcase` by default);
    # - `foreign_key` overrides the foreign key on the association (inferred as
    #   `name + "_id"` by default);
    # - `autosave` can be either:
    #   - `nil` (default) to only save newly built associations when the parent
    #     record is saved,
    #   - `true` to always save the associations (new or already persisted),
    #   - `false` to never save the associations automatically.
    # - `dependent` can be either:
    #   - `:delete` to `delete` the associated record in SQL,
    #   - `:destroy` to call `#destroy` on the associated object.
    macro belongs_to(name, class_name = nil, foreign_key = nil, autosave = nil, dependent = nil)
      {% unless class_name
           class_name = name.id.stringify.camelcase.id
         end %}
      {% unless foreign_key
           foreign_key = (name.id.stringify + "_id").id
         end %}

      @[JSON::Field(ignore: true)]
      @[YAML::Field(ignore: true)]
      @{{name.id}} : {{class_name}}?

      def {{name.id}} : {{class_name}}
        @{{name.id}} || reload_{{name.id}}
      end

      def {{name.id}}! : {{class_name}}
        {{name.id}}
      end

      def {{name.id}}? : {{class_name}}?
        @{{name.id}} || reload_{{name.id}}?
      end

      def {{name.id}}=(record : {{class_name}}) : {{class_name}}
        self.{{foreign_key.id}} = record.id.not_nil! unless record.new_record?
        @{{name.id}} = record
      end

      def build_{{name.id}}(**attributes) : {{class_name}}
        self.{{name.id}} = {{class_name}}.new(**attributes)
      end

      def create_{{name.id}}(**attributes) : {{class_name}}
        self.{{name.id}} = {{class_name}}.create(**attributes)
      end

      def create_{{name.id}}!(**attributes) : {{class_name}}
        self.{{name.id}} = {{class_name}}.create!(**attributes)
      end

      def reload_{{name.id}} : {{class_name}}
        @{{name.id}} = {{class_name}}.find({{foreign_key.id}}.not_nil!)
        @{{name.id}}.not_nil!
      end

      def reload_{{name.id}}? : {{class_name}}?
      @{{name.id}} = {{class_name}}.find?({{foreign_key.id}}.not_nil!)
      end

      def self.by_{{ foreign_key.id }}(id)
          self.where({{ foreign_key }}: id)
      end

      before_save do
        {% unless autosave == false %}
          if (%record = @{{name.id}}) {% if autosave == nil %} && %record.new_record? {% end %}
            %record.save
            self.{{foreign_key.id}} = %record.id.not_nil!
          end
        {% end %}
      end

      after_destroy do
        {% if dependent %}
          if {{foreign_key.id}}
            {% if dependent == :destroy %}
              self.{{name.id}}.try(&.destroy)
            {% elsif dependent == :delete %}
              {{class_name}}
              .where({ {{class_name}}.primary_key => {{foreign_key.id}} })
              .delete_all
            {% end %}
          end
        {% end %}
      end
    end

    # Declares a has one relationship.
    #
    # This will add the following methods:
    # - `association` returns the associated object (or nil).
    # - `association=` assigns the associated object, assigning the
    #   association's foreign key, then saving the association; permanently
    #   deletes the previously associated object;
    # - `reload_association` to reload the associated object.
    #
    # For example an Account class declares `has_one :supplier` which will add:
    #
    # - `Account#supplier` (similar to `Supplier.find_by(account_id: account.id)`)
    # - `Account#supplier=(supplier)` (similar to `supplier.account_id = account.id`)
    # - `Account#build_supplier`
    # - `Account#create_supplier`
    # - `Account#create_supplier!`
    # - `Account#reload_supplier`
    #
    # Options
    #
    # - `class_name` overrides the association class name (inferred as
    #   `name.camelcase` by default);
    # - `foreign_key` overrides the foreign key for the association (inferred as
    #   the name of this class + "_id" by default);
    # - `autosave` can be either:
    #   - `nil` (default) to only save newly built associations when the parent
    #     record is saved,
    #   - `true` to always save the associations (new or already persisted),
    #   - `false` to never save the associations automatically.
    # - `dependent` can be either:
    #   - `:nullify` (default) to set the foreign key to `nil` in SQL,
    #   - `:delete` to `delete` the associated record in SQL,
    #   - `:destroy` to call `#destroy` on the associated object.
    macro has_one(name, class_name = nil, foreign_key = nil, autosave = nil, dependent = nil)
      {% unless class_name
           class_name = name.id.stringify.camelcase.id
         end %}
      {% unless foreign_key
           foreign_key = (@type.id.stringify.split("::").last + "_id").underscore.id
         end %}

      @[JSON::Field(ignore: true)]
      @[YAML::Field(ignore: true)]
      @{{name.id}} : {{class_name}}?

      def {{name.id}} : {{class_name}}
        @{{name.id}} || reload_{{name.id}}
      end

      def {{name.id}}! : {{class_name}}
        {{name.id}}
      end

      def {{name.id}}? : {{class_name}}?
        @{{name.id}} || reload_{{name.id}}?
      end

      def {{name.id}}=(record : {{class_name}}) : {{class_name}}
        unless new_record?

          case {{dependent}}
          when :delete
            {{class_name}}.where({{foreign_key.id}}: id).delete_all
          when :destroy
            if %assoc = @{{name.id}}
              %assoc.destroy
            else
              {{class_name}}.where({{foreign_key.id}}: id).take?.try(&.destroy)
            end
          else # :nullify
            {{class_name}}.where({{foreign_key.id}}: id).update_all({{foreign_key.id}}: nil)
          end

          record.{{foreign_key.id}} = id.not_nil!
          record.save
        end
        @{{name.id}} = record
      end

      def build_{{name.id}}(**attributes) : {{class_name}}
        record = {{class_name}}.new(**attributes)
        record.{{foreign_key.id}} = id unless new_record?
        @{{name.id}} = record
      end

      def create_{{name.id}}(**attributes) : {{class_name}}
        raise ::PgORM::Error::RecordNotSaved.new("can't initialize {{class_name}} for #{self.class.name} doesn't have an id.") unless id?
        build_{{name.id}}(**attributes).tap(&.save)
      end

      def create_{{name.id}}!(**attributes) : {{class_name}}
        raise ::PgORM::Error::RecordNotSaved.new("can't initialize {{class_name}} for #{self.class.name} doesn't have an id.") unless id?
        build_{{name.id}}(**attributes).tap(&.save!)
      end

      def reload_{{name.id}} : {{class_name}}
        @{{name.id}} = {{class_name}}.find_by({{foreign_key}}: id)
      end

      def reload_{{name.id}}? : {{class_name}}?
        @{{name.id}} = {{class_name}}.find_by?({{foreign_key}}: id)
      end

      after_save do
        {% unless autosave == false %}
          if (%record = @{{name.id}}) {% if autosave == nil %} && %record.new_record? {% end %}
            %record.{{foreign_key.id}} = id
            %record.save
          end
        {% end %}
      end

      after_destroy do
        {% if dependent == :destroy %}
          self.{{name.id}}.try(&.destroy)
        {% elsif dependent == :delete %}
        {{class_name}}
          .where({{foreign_key}}: id)
          .delete_all
        {% elsif dependent == :nullify %}
        {{class_name}}
          .where({{foreign_key}}: id)
          .update_all({{foreign_key}}: nil)
        {% end %}
      end
    end

    # Declares a has many relationship.
    macro has_many(name, class_name = nil, foreign_key = nil, autosave = nil, dependent = nil, serialize = false)
      {% unless class_name
           class_name = name.id.stringify.gsub(/s$/, "").camelcase.id
         end %}
      {% unless foreign_key
           foreign_key = (@type.id.stringify.split("::").last.gsub(/s$/, "") + "_id").underscore.id
         end %}

      {% relation_var = ("__" + name.id.stringify + "_rel").id %}

      @[JSON::Field(ignore: true)]
      @[YAML::Field(ignore: true)]
      @{{name.id}} : ::PgORM::Relation({{class_name}})?

      {% if serialize %}
        @[::PgORM::Associations::SerializeMarker(key: {{relation_var.id}}, cache: {{name.id}})]
        @[JSON::Field(key: {{name.id}}, ignore_deserialize: true)]
        getter({{relation_var.id}} : Array({{class_name}})){ {{name.id}}.to_a || Array({{class_name}}).new }
      {% end %}

      def {{name.id}} : ::PgORM::Relation({{class_name}})
        @{{name.id}} ||= ::PgORM::Relation({{class_name}}).new(self, {{foreign_key.id.symbolize}})
      end

      after_save do
        {% unless autosave == false %}
        if %records = @{{name.id}}
          if %records.cached?
            %records.each do |%record|
              {% if autosave == nil %} next unless %record.new_record? {% end %}
              %record.{{foreign_key.id}} = id.not_nil!
              %record.save
            end
          end
        end
        {% end %}
      end

      after_destroy do
        {% if dependent == :destroy %}
          self.{{name.id}}.try(&.each(&.destroy))
        {% elsif dependent == :delete_all %}
        {{class_name}}
          .where({{foreign_key}}: id)
          .delete_all
        {% elsif dependent == :nullify %}
        {{class_name}}
          .where({{foreign_key}}: id)
          .update_all({{foreign_key}}: nil)
        {% end %}
      end
    end

    macro __process_assoc_serialization__
      {% props = [] of {Nil, Nil} %}
      {% for ivar in @type.instance_vars %}
        {% ann = ivar.annotation(::PgORM::Associations::SerializeMarker) %}
        {% if ann && ann[:key] %}
          {% props << {ann[:key], ann[:cache]} %}
        {% end %}
      {% end %}
      {% for ivar in props %}
        {{ivar[0]}} if {{ivar[1]}}.try &.cached?
      {% end %}
    end

    macro included
      macro inherited
        macro finished
          def invoke_props
            __process_assoc_serialization__
          end
        end
      end
    end
  end
end

module PgORM::Validators
  # In case of transformations on field, allow user defined transform
  macro ensure_unique(field, scope = [] of Nil, callback = nil, &transform)
      validate {{ field }}, "should be unique", ->(this: self) do
        {% if scope.empty? %}
          {% scope = [field] %}
          {% proc_return_type = FIELDS[field.id][:klass].union_types.reject(&.==(Nil)).join('|').id %}
        {% else %}
          {% proc_return_type = "Tuple(#{scope.map { |s| FIELDS[s.id][:klass].union_types.reject(&.==(Nil)).join('|').id }.join(", ").id})".id %}
        {% end %}

        # Return if any values are nil
        {% for s in scope %}
          return true if this.{{s.id}}.nil?
        {% end %}

        # Construct proc type fron scope array
        # Arguments are not-nillable as nil status is checked above.
        {% proc_arg_type = "#{scope.map { |s| FIELDS[s.id][:klass].union_types.reject(&.==(Nil)).join('|').id }.join(", ").id}".id %}
        {% signature = "#{scope.map { |s| "#{s.id}: #{FIELDS[s.id][:klass].union_types.reject(&.==(Nil)).join('|').id}" }.join(", ").id}".id %}

        # Handle Transformation block/callback
        {% if transform %}
          # Construct a proc from a given block, call with argument.
          transform_proc : Proc({{ proc_arg_type }}, {{ proc_return_type }}) = ->({{ signature.id }}) { {{ transform.body }} }

          result : {{ proc_return_type }} = transform_proc.call(
          {% for s in scope %}this.{{s.id}}.not_nil!,{% end %}
          )
          {% if scope.size == 1 %}
            %results = __convert_to_db_col_value({{scope.first.id}}, result)
          {% else %}
              %results = { {% for s, index in scope %}__convert_to_db_col_value({{s.id}},result[{{index}}]),{% end %}}
          {% end %}
        {% elsif callback %}
          result : {{ proc_return_type }} = this.{{ callback.id }}(
            {% for s in scope %}this.{{s.id}}.not_nil!,{% end %}
          )
          {% if scope.size == 1 %}
            %results = __convert_to_db_col_value({{scope.first.id}}, result)
          {% else %}
              %results = { {% for s, index in scope %}__convert_to_db_col_value({{s.id}},result[{{index}}]),{% end %}}
          {% end %}
        {% else %}
          {% if scope.size == 1 %}
            # No transform
            %results = {
              {% for s in scope %}__convert_to_db_col_value({{s.id}},this.{{s.id}}),{% end %}
            }
          {% else %}
          %results = __convert_to_db_col_value({{scope.first.id}}, {{scope.first.id}})
          {% end %}
        {% end %}

        # Fetch Record
        {% if scope.size == 1 %}
            rval = case %results
            when Tuple then %results.to_a
            when String then %results.as(String)
            end
            doc = self.where({{field.id}}: rval).first?
        {% else %}
          # Where query with all scoped fields
          doc = self.where(
            {% for s, index in scope %} {{s.id}}: %results[{{ index.id }}], {% end %}
          ).first?
        {% end %}

        # Fields are not present in another record under present table
        success = !(doc && doc.id != this.id)
        {% if transform || callback %}
          # Set fields in unique scope with result of transform block if record is unique
          if success && !this.persisted?
            {% if scope.size == 1 %}
              this.{{ field.id }} = result
            {% else %}
              {% for s, index in scope %}
                this.{{ s.id }} = result[{{ index.id }}]
              {% end %}
            {% end %}
          end
        {% end %}

        success
      end
    end

  # :nodoc:
  private macro __convert_to_db_col_value(field, value)
      {% opts = FIELDS[field.id] %}
      {% if opts[:klass] < Array && !opts[:converter] %}
        PQ::Param.encode_array({{value}} || ([] of {{opts[:klass]}}))
      {% elsif opts[:klass] < Set %}
        PQ::Param.encode_array(({{value}} || (Set({{opts[:klass]}}).new)).to_a)
      {% elsif opts[:klass].union_types.reject(&.==(Nil)).first < Enum %}
        {{value}}.try &.value
      {% elsif (::PgORM::Value).union_types.includes?(opts[:klass].union_types.reject(&.==(Nil)).first) %}
        {{value}}
      {% elsif opts[:converter] %}
        {{opts[:converter]}}.to_json(@{{value}})
      {% else %}
        {{value}}.to_json
      {% end %}
    end
end

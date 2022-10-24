module PgORM::Table
  macro included
    macro inherited
      # :nodoc:
      TABLE_NAME = {} of Symbol => String
      __set_default_table__
    end
  end

  private macro __set_default_table__
      table({{ @type.name.gsub(/::/, "_").underscore }})
    end

  # Macro to manually set the table name of the model
  macro table(name)
      {% TABLE_NAME[:name] = name.id.stringify %}
    end

  macro __process_table__
      {% unless PgORM::Base::TABLES.includes?(TABLE_NAME[:name]) %}
        {% PgORM::Base::TABLES << TABLE_NAME[:name] %}
      {% end %}

      @@table_name : String = {{ TABLE_NAME[:name] }}

      def self.table_name
        @@table_name
      end

      def table_name
        @@table_name
      end
    end
end

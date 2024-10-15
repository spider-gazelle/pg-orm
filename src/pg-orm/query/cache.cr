require "./iterator"

module PgORM
  module Query
    module Cache(T)
      @cache : Array(T)?

      def to_a : Array(T)
        @cache ||= begin
          if (val = parent) && (json = val.extra_attributes["#{T.table_name}_join_result"]?)
            Array(T).from_json(json.to_s).tap(&.each { |entry|
              entry.new_record = false
              entry.clear_changes_information
            })
          else
            Database.adapter(builder).select_all { |rs| T.new(rs) }
          end
        end
      end

      # Iterates all records loaded from the database.
      def each(& : T ->) : Nil
        to_a.each { |row| yield row }
      end

      # Iterates all records if previously loaded from the database, or iterates
      # records directly streamed from the database otherwise.
      def each
        if cache = @cache
          cache.each
        else
          Iterator(T).new(builder)
        end
      end

      def reload
        @cache = nil
        to_a
      end

      def cached?
        !(@cache.nil? && parent.try &.extra_attributes["#{T.table_name}_join_result"]?.nil?)
      end

      # :nodoc:
      def parent
        nil
      end
    end
  end
end

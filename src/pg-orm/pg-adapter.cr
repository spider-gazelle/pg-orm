require "pg"
require "uuid"
require "uuid/json"

module PgORM
  struct PostgreSQL
    private getter builder : Query::Builder

    def initialize(@builder)
    end

    def self.quote(name : Symbol | String, io : IO)
      io << PG::EscapeHelper.escape_identifier(name.to_s)
    end

    def quote(name : Symbol | String, io : IO)
      self.class.quote(name, io)
    end

    def quote(name : Symbol | String)
      String.build do |sb|
        quote(name, sb)
      end
    end

    def insert(attributes : Hash | NamedTuple, &)
      sql, args = insert_sql(attributes)
      Database.connection &.query_one(sql, args: args) do |rs|
        yield rs.read
      end
    end

    def select_one(&)
      return if @builder.none?
      sql, args = select_sql
      Database.connection &.query_one?(sql, args: args) { |rs| yield rs }
    end

    def select_all(& : DB::ResultSet -> U) : Array(U) forall U
      if @builder.none?
        Array(U).new(0)
      else
        sql, args = select_sql
        Database.connection &.query_all(sql, args: args) { |rs| yield rs }
      end
    end

    def select_each(&) : Nil
      return if @builder.none?
      sql, args = select_sql
      Database.connection &.query_each(sql, args: args) { |rs| yield rs }
    end

    def scalar
      sql, args = select_sql
      Database.connection &.scalar(sql, args: args)
    end

    def update(attributes : Hash | NamedTuple) : Nil
      return if @builder.none?
      sql, args = update_sql(attributes)
      Database.connection &.exec(sql, args: args)
    end

    def delete : Nil
      return if @builder.none?
      sql, args = delete_sql
      Database.connection &.exec(sql, args: args)
    end

    def to_sql : String
      sql, _ = select_sql
      sql
    end

    protected def insert_sql(attributes) : {String, Array(Value)}
      args = [] of Value
      sql = String.build do |str|
        build_insert(attributes, str, args)
      end
      {sql, args}
    end

    protected def select_sql
      args = [] of Value
      sql = String.build do |str|
        build_select(str)
        build_join(str)
        build_where(str, args)
        build_groups(str)
        build_order_by(str)
        build_limit(str)
      end
      {sql, args}
    end

    protected def update_sql(attributes) : {String, Array(Value)}
      args = [] of Value
      sql = String.build do |str|
        build_update(attributes, str, args)
        build_where(str, args)
      end
      {sql, args}
    end

    protected def delete_sql : {String, Array(Value)}
      args = [] of Value
      sql = String.build do |str|
        build_delete(str)
        build_where(str, args)
      end
      {sql, args}
    end

    protected def build_join(io) : Nil
      if joins = builder.joins?
        joins.each do |cond|
          case cond
          when Tuple(JoinType, String, String, String)
            type, jtable, key, fkey = cond
            io << type.to_sql
            quote(jtable, io)
            io << " ON " << "#{jtable}.#{fkey} = " << key
            io << " "
          when Tuple(JoinType, String, String)
            type, jtable, on = cond
            io << type.to_sql
            quote(jtable, io)
            io << " ON " << on
            io << " "
          end
        end
      end
    end

    protected def build_groups(io) : Nil
      if groups = builder.groups?
        io << " GROUP BY "
        groups.each_with_index do |column_name, index|
          io << ", " unless index == 0
          case column_name
          when Symbol
            quote(column_name, io)
          when String
            io << column_name
          end
        end
      end
    end

    protected def build_select(io) : Nil
      io << "SELECT "
      io << "DISTINCT " if builder.distinct?

      if selects = builder.selects?
        selects.each_with_index do |column_name, index|
          io << ", " unless index == 0
          case column_name
          when Symbol
            quote(column_name, io)
          when String
            io << column_name
          end
        end
      else
        io << '*'
      end

      # Add rank column if full-text search with ranking is used
      if rank_col = builder.fts_rank_column
        io << ", " if selects || true
        build_fts_rank_select(io)
      end

      io << " FROM "
      quote(builder.table_name, io)
    end

    protected def build_fts_rank_select(io) : Nil
      return unless conditions = builder.conditions?

      fts_condition = conditions.find { |c| c.is_a?(Query::Builder::FullTextSearchCondition) }
      return unless fts_condition.is_a?(Query::Builder::FullTextSearchCondition)

      search_query = fts_condition.search_query

      # Use the specified rank function (ts_rank or ts_rank_cd)
      io << search_query.rank_function.to_s << "("

      # Build tsvector (with or without weights)
      if weighted_cols = search_query.weighted_columns
        build_weighted_tsvector(io, weighted_cols, search_query.config)
      else
        build_tsvector_concat(io, search_query.columns, search_query.config)
      end

      io << ", "

      # Build tsquery
      build_tsquery(io, search_query)

      if norm = search_query.rank_normalization
        io << ", " << norm
      end

      io << ") AS ts_rank"
    end

    protected def build_tsvector_concat(io, columns : Array(String), config : String)
      columns.each_with_index do |col, idx|
        io << " || ' ' || " unless idx == 0
        io << "to_tsvector('" << config << "', COALESCE("
        quote(col, io)
        io << ", ''))"
      end
    end

    protected def build_weighted_tsvector(io, weighted_columns : Array(FullTextSearch::WeightedColumn), config : String)
      weighted_columns.each_with_index do |wcol, idx|
        io << " || " unless idx == 0
        io << "setweight(to_tsvector('" << config << "', COALESCE("
        quote(wcol.column, io)
        io << ", '')), '" << wcol.weight.to_char << "')"
      end
    end

    protected def build_tsquery(io, search_query : FullTextSearch::SearchQuery)
      if search_query.use_plain_query?
        io << "plainto_tsquery('" << search_query.config << "', "
      else
        io << "to_tsquery('" << search_query.config << "', "
      end

      # Escape single quotes in the query string
      escaped_query = search_query.query.gsub("'", "''")
      io << '\'' << escaped_query << '\''
      io << ')'
    end

    private def build_insert(attributes : Hash, io, args)
      io << "INSERT INTO "
      quote(builder.table_name, io)

      if attributes.empty?
        io << " DEFAULT VALUES"
      else
        io << " ("

        attributes.each_with_index do |(column_name, _), index|
          io << ", " unless index == 0
          quote(column_name, io) unless column_name.nil?
        end

        io << ") VALUES ("
        attributes.each_with_index do |(_, value), index|
          args << value
          io << ", " unless index == 0
          io << '$' << args.size
        end

        io << ')'
      end

      io << " RETURNING "
      quote(builder.primary_key, io)
    end

    private def build_insert(attributes : NamedTuple, io, args)
      io << "INSERT INTO "
      quote(builder.table_name, io)

      if attributes.empty?
        io << "DEFAULT VALUES"
      else
        io << " ("

        attributes.each_with_index do |column_name, _, index|
          io << ", " unless index == 0
          quote(column_name, io)
        end

        io << ") VALUES ("
        attributes.each_with_index do |_, value, index|
          args << value
          io << ", " unless index == 0
          io << '$' << args.size
        end
        io << ')'
      end

      io << " RETURNING "
      quote(builder.primary_key, io)
    end

    private def build_insert_default_values(io)
      io << " DEFAULT VALUES"
    end

    private def build_update(attributes : Hash, io, args)
      io << "UPDATE "
      quote(builder.table_name, io)
      io << " SET "
      attributes.each_with_index do |(column_name, value), index|
        args << value
        io << ", " unless index == 0
        quote(column_name, io)
        io << " = $" << args.size
      end
    end

    private def build_update(attributes : NamedTuple, io, args)
      io << "UPDATE "
      quote(builder.table_name, io)
      io << " SET "
      attributes.each_with_index do |column_name, value, index|
        if value.is_a?(Array)
          args << "{#{value.as(Array).join}}"
        else
          args << value
        end
        io << ", " unless index == 0
        col = quote(column_name)
        io << col
        if value.is_a?(Array)
          io << " = #{col} || $" << args.size
        else
          io << " = $" << args.size
        end
      end
    end

    private def build_delete(io : IO) : Nil
      io << "DELETE FROM "
      quote(builder.table_name, io)
    end

    protected def build_where(io, args) : Nil
      return unless conditions = builder.conditions?
      has_join = !builder.joins?.nil?
      io << " WHERE "
      build_where_conditions(conditions, io, args, has_join)
    end

    private def build_where_conditions(conditions, io, args, has_join = false, first = true) : Nil
      conditions.each_with_index do |condition, index|
        io << " AND " if index != 0 && first

        case condition
        when Query::Builder::Condition
          if has_join
            io << builder.table_name << "." << condition.column_name
          else
            quote(condition.column_name, io)
          end

          case value = condition.value
          when Array(Value)
            if condition.not?
              io << " NOT IN ("
            else
              io << " IN ("
            end
            value.size.times do |vindex|
              io << ", " unless vindex == 0
              io << '$' << args.size + vindex + 1
            end
            io << ')'
            args.concat(value)
          when nil
            if condition.not?
              io << " IS NOT NULL"
            else
              io << " IS NULL"
            end
          when Regex
            args << value.source
            io << ' '
            io << '!' if condition.not?
            io << '~'
            io << '*' if value.options.ignore_case?
            io << " $" << args.size
          else
            args << value
            if condition.not?
              io << " <> $" << args.size
            else
              io << " = $" << args.size
            end
          end
        when Query::Builder::RawCondition
          io << "NOT " if condition.not?
          io << '('

          if values = condition.values
            n = args.size
            args.concat(values)
            io << condition.raw.gsub("?") { "$#{n += 1}" }
          else
            io << condition.raw
          end

          io << ')'
        when Query::Builder::FullTextSearchCondition
          search_query = condition.search_query
          io << '('

          # Build tsvector (with or without weights)
          if weighted_cols = search_query.weighted_columns
            build_weighted_tsvector(io, weighted_cols, search_query.config)
          else
            build_tsvector_concat(io, search_query.columns, search_query.config)
          end

          io << ") @@ "

          # Build tsquery
          build_tsquery(io, search_query)
        when Query::Builder::OrCondition
          io << '('
          build_where_conditions(condition.left_conditions, io, args, has_join, true)
          io << ") OR ("
          build_where_conditions(condition.right_conditions, io, args, has_join, true)
          io << ')'
        end
      end
    end

    def build_where_regex(condition, io, args)
      args << condition.value.as(Regex).source
      io << "NOT (" if condition.not?
      quote(condition.column_name, io)
      io << " REGEXP ?"
      io << ')' if condition.not?
    end

    protected def build_order_by(io) : Nil
      return unless orders = builder.orders?

      io << " ORDER BY "
      orders.each_with_index do |order, index|
        io << ", " unless index == 0

        case order
        when {Symbol, Symbol}
          column_name, direction = order.as({Symbol, Symbol})
          quote(column_name, io)
          case direction
          when :asc  then io << " ASC"
          when :desc then io << " DESC"
          end
        when String
          io << order
        end
      end
    end

    protected def build_limit(io) : Nil
      if limit = builder.limit?
        io << " LIMIT " << limit
      end
      if offset = builder.offset?
        io << " OFFSET " << offset
      end
    end
  end
end

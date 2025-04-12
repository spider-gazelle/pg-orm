require "./collection"

module PgORM
  # Most methods are delegated to a `Collection(T)` object.
  module Query
    private def query : Collection(self)
      {% begin %}
        Collection({{@type}}).new(Builder.new(table_name, primary_key.to_s))
      {% end %}
    end

    # Loads records by raw SQL query. You may refer to arguments with `$x` where `x` is the number in the
    # SQL query, and pass them to the method. For example:
    #
    # ```
    # users = User.find_all_by_sql(<<-SQL, "user")
    #   SELECT * FROM "users" WHERE username = $0
    #   SQL
    # ```
    def find_all_by_sql(sql : String, *args_, args : Array? = nil) : Array(self)
      Database.connection &.query_all(sql, *args_, args: args) { |rs| new(rs) }
    end

    # Loads one record by raw SQL query. You may refer to arguments with `$x` where `x` is the number  in
    # the SQL query, and pass them to the method. For example:
    #
    # ```
    # user = User.find_one_by_sql(<<-SQL, "user")
    #   SELECT * FROM "users" WHERE username = $0 LIMIT 1
    #   SQL
    # ```
    #
    # Raises a `Error::RecordNotFound` exception when no record could be found in the
    # database.
    def find_one_by_sql(sql : String, *args_, args : Array? = nil) : self
      Database.connection &.query_one?(sql, *args_, args: args) { |rs| new(rs) } || raise Error::RecordNotFound.new
    end

    # Same as `#find_one_by_sql` but returns `nil` when no record could be found
    # in the database.
    def find_one_by_sql?(sql : String, *args_, args : Array? = nil) : self?
      Database.connection &.query_one?(sql, *args_, args: args) { |rs| new(rs) }
    end

    def none : Collection(self)
      query.none
    end

    def all : Collection(self)
      query.all
    end

    def ids : Array
      query.ids
    end

    def find(id) : self
      query.find(id)
    end

    def find_all(ids : Enumerable(Value)) : Collection(self)
      return none if ids.empty?
      case keys = primary_key
      when Symbol
        where({keys => ids.to_a})
      else
        raise ArgumentError.new("must provide multiple key ids for tables with composite keys")
      end
    end

    def find_all(ids : Enumerable(Enumerable)) : Collection(self)
      return none if ids.empty?
      case keys = primary_key
      when Tuple
        # might be able to optimise this by checking if individual id components
        # already exist in the data array and re-using the `$indexes` but probably
        # not worth the effort
        data = [] of Value

        # WHERE (primary1, primary2) IN ((val1, val2), (val3, val4))
        where(String.build { |io|
          io << '('
          keys.each_with_index do |key, index|
            io << ", " unless index.zero?
            io << PG::EscapeHelper.escape_identifier(key.to_s)
          end
          io << ") IN ("

          ids.each_with_index do |id, idx|
            io << ", " unless idx.zero?
            io << '('

            if id.responds_to?(:each_with_index)
              id.each_with_index do |component, index|
                io << ", " unless index.zero?
                data << component
                io << '?'
              end
            end
            io << ')'
          end
          io << ')'
        }, args: data)
      else
        raise ArgumentError.new("multiple key ids are only supported on composite key tables")
      end
    end

    def find?(id) : self?
      query.find?(id)
    end

    def find!(id) : self
      query.find?(id) || raise Error::RecordNotFound.new("Key not present: #{id}")
    end

    def find_by(**args) : self
      query.find_by(**args)
    end

    def find_by?(**args) : self?
      query.find_by?(**args)
    end

    def exists?(id) : Bool
      query.exists?(id)
    end

    def take : self
      query.take
    end

    def take? : self?
      query.take?
    end

    def first : self
      query.first
    end

    def first? : self?
      query.first?
    end

    def last : self
      query.last
    end

    def last? : self?
      query.last?
    end

    def pluck(column_name : Symbol | String) : Array(Value)
      query.pluck(column_name)
    end

    def count(column_name : Symbol | String = "*", distinct = false) : Int64
      query.count(column_name, distinct)
    end

    def sum(column_name : Symbol | String) : Int64 | Float64
      query.sum(column_name)
    end

    def average(column_name : Symbol | String) : Float64
      query.average(column_name)
    end

    def minimum(column_name : Symbol | String)
      query.minimum(column_name)
    end

    def maximum(column_name : Symbol | String)
      query.maximum(column_name)
    end

    def select(*columns : Symbol) : Collection(self)
      query.select(*columns)
    end

    def select(sql : String) : Collection(self)
      query.select(sql)
    end

    def distinct(value = true) : Collection(self)
      query.distinct(value)
    end

    def where(sql : String) : Collection(self)
      query.where(raw: sql)
    end

    def where(conditions : Hash(Symbol, Value | Array(Value))) : Collection(self)
      {% begin %}
      query.where(conditions).as(Collection({{@type}}))
      {% end %}
    end

    def where(**conditions) : Collection(self)
      query.where(**conditions)
    end

    def where(sql : String, *splat : Value) : Collection(self)
      query.where(sql, *splat)
    end

    def where(sql : String, args : Enumerable) : Collection(self)
      query.where(sql, args: args)
    end

    def where_not(conditions : Hash(Symbol, Value | Array(Value))) : Collection(self)
      query.where_not(conditions)
    end

    def where_not(**conditions) : Collection(self)
      query.where_not(**conditions)
    end

    def limit(value : Int32) : Collection(self)
      query.limit(value)
    end

    def offset(value : Int32) : Collection(self)
      query.offset(value)
    end

    def join(type : JoinType, model : Base.class, fk : Symbol, pk : Base.class | Nil = nil) : Collection(self)
      query.join(type, model, fk, pk)
    end

    def join(type : JoinType, model : Base.class, on : String) : Collection(self)
      query.join(type, model, on)
    end

    def group_by(*columns : Symbol | String) : Collection(self)
      query.join(*columns)
    end

    def order(columns : Hash(Symbol, Symbol)) : Collection(self)
      query.order(columns)
    end

    def order(*columns : Symbol | String) : Collection(self)
      query.order(*columns)
    end

    def order(**columns) : Collection(self)
      query.order(**columns)
    end

    def reorder(columns : Hash(Symbol, Symbol)) : Collection(self)
      query.reorder(columns)
    end

    def reorder(*columns : Symbol | String) : Collection(self)
      query.reorder(*columns)
    end

    def reorder(**columns) : Collection(self)
      builder.reorder(**columns)
    end
  end
end

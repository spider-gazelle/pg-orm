require "./collection"

module PgORM
  # Query module provides the primary interface for building and executing database queries.
  # Most methods are delegated to a `Collection(T)` object which provides a chainable,
  # immutable query builder interface.
  #
  # ## Basic Usage
  #
  # ```
  # # Find all users
  # User.all.to_a
  #
  # # Find with conditions
  # User.where(active: true).order(:name).limit(10).to_a
  #
  # # Find by ID
  # user = User.find(123)
  #
  # # Aggregate queries
  # User.where(active: true).count
  # User.sum(:age)
  # ```
  module Query
    private def query : Collection(self)
      {% begin %}
        Collection({{@type}}).new(Builder.new(table_name, primary_key.to_s))
      {% end %}
    end

    # Loads records by raw SQL query with parameter binding.
    #
    # Use `$1`, `$2`, etc. for parameter placeholders (PostgreSQL style).
    # Parameters are passed as additional arguments after the SQL string.
    #
    # ## Example
    #
    # ```
    # # Single parameter
    # users = User.find_all_by_sql("SELECT * FROM users WHERE username = $1", "john")
    #
    # # Multiple parameters
    # users = User.find_all_by_sql(
    #   "SELECT * FROM users WHERE age > $1 AND city = $2",
    #   18,
    #   "NYC"
    # )
    #
    # # With array of parameters
    # params = ["john", "doe"]
    # users = User.find_all_by_sql("SELECT * FROM users WHERE first_name = $1 OR last_name = $2", args: params)
    # ```
    #
    # Returns an array of model instances.
    def find_all_by_sql(sql : String, *args_, args : Array? = nil) : Array(self)
      Database.connection &.query_all(sql, *args_, args: args) { |rs| new(rs) }
    end

    # Loads one record by raw SQL query with parameter binding.
    #
    # Use `$1`, `$2`, etc. for parameter placeholders (PostgreSQL style).
    # Always include `LIMIT 1` in your SQL for performance.
    #
    # ## Example
    #
    # ```
    # user = User.find_one_by_sql(
    #   "SELECT * FROM users WHERE username = $1 LIMIT 1",
    #   "john"
    # )
    # ```
    #
    # Raises `Error::RecordNotFound` if no record is found.
    def find_one_by_sql(sql : String, *args_, args : Array? = nil) : self
      Database.connection &.query_one?(sql, *args_, args: args) { |rs| new(rs) } || raise Error::RecordNotFound.new
    end

    # Same as `#find_one_by_sql` but returns `nil` when no record is found
    # instead of raising an exception.
    #
    # ## Example
    #
    # ```
    # user = User.find_one_by_sql?(
    #   "SELECT * FROM users WHERE username = $1 LIMIT 1",
    #   "john"
    # )
    #
    # if user
    #   puts "Found: #{user.name}"
    # else
    #   puts "Not found"
    # end
    # ```
    def find_one_by_sql?(sql : String, *args_, args : Array? = nil) : self?
      Database.connection &.query_one?(sql, *args_, args: args) { |rs| new(rs) }
    end

    # Returns a collection that will never return any records.
    #
    # Useful for conditional query building where you want to ensure
    # no results are returned in certain cases.
    #
    # ## Example
    #
    # ```
    # scope = user.admin? ? User.all : User.none
    # scope.to_a # => [] (if not admin)
    # ```
    def none : Collection(self)
      query.none
    end

    # Returns a collection representing all records in the table.
    #
    # This doesn't execute a query immediately - it returns a lazy collection
    # that will query the database when you call a terminating method like
    # `.to_a`, `.each`, `.first`, etc.
    #
    # ## Example
    #
    # ```
    # users = User.all # No query executed yet
    # users.to_a       # Now the query executes
    # ```
    def all : Collection(self)
      query.all
    end

    # Loads all primary key values matching the current query scope.
    #
    # This is more efficient than loading full records when you only need IDs.
    # For composite primary keys, returns tuples.
    #
    # ## Example
    #
    # ```
    # # Simple primary key
    # user_ids = User.where(active: true).ids
    # # => [1, 2, 3, 4, 5]
    #
    # # Composite primary key
    # keys = CompositeModel.where(status: "active").ids
    # # => [{key1: "a", key2: 1}, {key1: "b", key2: 2}]
    # ```
    def ids : Array
      query.ids
    end

    # Finds a record by its primary key value.
    #
    # For single primary keys, pass the ID directly.
    # For composite primary keys, pass a tuple of values.
    #
    # ## Example
    #
    # ```
    # # Single primary key
    # user = User.find(123)
    #
    # # Composite primary key
    # record = CompositeModel.find({"key1", "key2"})
    # ```
    #
    # Raises `Error::RecordNotFound` if the record doesn't exist.
    def find(id) : self
      query.find(id)
    end

    # Finds multiple records by an array of primary key values.
    #
    # Returns a collection (not an array) so you can chain additional query methods.
    # For models with single primary keys only.
    #
    # ## Example
    #
    # ```
    # users = User.find_all([1, 2, 3, 4, 5])
    # active_users = User.find_all([1, 2, 3]).where(active: true)
    # ```
    def find_all(ids : Enumerable(Value)) : Collection(self)
      return none if ids.empty?
      case keys = primary_key
      when Symbol
        where({keys => ids.to_a})
      else
        raise ArgumentError.new("must provide multiple key ids for tables with composite keys")
      end
    end

    # Finds multiple records by composite primary key values.
    #
    # Pass an array of tuples/arrays, where each tuple contains the values
    # for all components of the composite key.
    #
    # ## Example
    #
    # ```
    # # For a model with primary_key :tenant_id, :user_id
    # records = CompositeModel.find_all([
    #   {1, 100},
    #   {1, 101},
    #   {2, 200},
    # ])
    # ```
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

    # Same as `#find` but returns `nil` if the record doesn't exist
    # instead of raising an exception.
    #
    # ## Example
    #
    # ```
    # user = User.find?(123)
    # puts user ? user.name : "Not found"
    # ```
    def find?(id) : self?
      query.find?(id)
    end

    # Same as `#find` but with explicit exception raising.
    #
    # Useful when you want to be explicit about the error handling.
    #
    # ## Example
    #
    # ```
    # user = User.find!(123) # Raises Error::RecordNotFound if not found
    # ```
    def find!(id) : self
      query.find?(id) || raise Error::RecordNotFound.new("Key not present: #{id}")
    end

    # Finds the first record matching the given conditions.
    #
    # ## Example
    #
    # ```
    # user = User.find_by(email: "john@example.com")
    # user = User.find_by(name: "John", active: true)
    # ```
    #
    # Raises `Error::RecordNotFound` if no matching record is found.
    def find_by(**args) : self
      query.find_by(**args)
    end

    # Same as `#find_by` but returns `nil` if no record is found.
    #
    # ## Example
    #
    # ```
    # user = User.find_by?(email: "john@example.com")
    # if user
    #   puts "Found: #{user.name}"
    # end
    # ```
    def find_by?(**args) : self?
      query.find_by?(**args)
    end

    # Checks if a record with the given primary key exists.
    #
    # More efficient than loading the full record when you only need
    # to check existence.
    #
    # ## Example
    #
    # ```
    # if User.exists?(123)
    #   puts "User exists"
    # end
    #
    # # With scope
    # if User.where(active: true).exists?(123)
    #   puts "Active user exists"
    # end
    # ```
    def exists?(id) : Bool
      query.exists?(id)
    end

    # Returns one record without any specific ordering.
    #
    # Faster than `first` when you don't care about order.
    # Useful for checking if any records exist.
    #
    # ## Example
    #
    # ```
    # user = User.where(active: true).take
    # ```
    #
    # Raises `Error::RecordNotFound` if no records exist.
    def take : self
      query.take
    end

    # Same as `#take` but returns `nil` if no records exist.
    #
    # ## Example
    #
    # ```
    # user = User.where(active: true).take?
    # puts user ? user.name : "No active users"
    # ```
    def take? : self?
      query.take?
    end

    # Returns the first record, ordered by primary key ascending.
    #
    # If an order is already specified, uses that ordering instead.
    #
    # ## Example
    #
    # ```
    # user = User.first                          # ORDER BY id ASC
    # user = User.order(:name).first             # ORDER BY name ASC
    # user = User.order(created_at: :desc).first # Uses specified order
    # ```
    #
    # Raises `Error::RecordNotFound` if no records exist.
    def first : self
      query.first
    end

    # Same as `#first` but returns `nil` if no records exist.
    def first? : self?
      query.first?
    end

    # Returns the last record, ordered by primary key descending.
    #
    # If an order is already specified, reverses that ordering.
    #
    # ## Example
    #
    # ```
    # user = User.last                          # ORDER BY id DESC
    # user = User.order(:name).last             # ORDER BY name DESC
    # user = User.order(created_at: :desc).last # ORDER BY created_at ASC
    # ```
    #
    # Raises `Error::RecordNotFound` if no records exist.
    def last : self
      query.last
    end

    # Same as `#last` but returns `nil` if no records exist.
    def last? : self?
      query.last?
    end

    # Extracts values from a single column as an array.
    #
    # More efficient than loading full records when you only need one column.
    # Can also be used with raw SQL expressions.
    #
    # ## Example
    #
    # ```
    # # Column name
    # names = User.where(active: true).pluck(:name)
    # # => ["John", "Jane", "Bob"]
    #
    # # Multiple queries
    # emails = User.pluck(:email)
    # ages = User.pluck(:age)
    #
    # # Works with scopes
    # admin_names = User.where(role: "admin").pluck(:name)
    # ```
    def pluck(column_name : Symbol | String) : Array(Value)
      query.pluck(column_name)
    end

    # Counts the number of records matching the current query scope.
    #
    # Can count all records, a specific column, or distinct values.
    #
    # ## Example
    #
    # ```
    # # Count all users
    # User.count # => 150
    #
    # # Count with conditions
    # User.where(active: true).count # => 120
    #
    # # Count specific column
    # User.count(:email) # => 150
    #
    # # Count distinct values
    # User.count(:city, distinct: true) # => 25
    # ```
    def count(column_name : Symbol | String = "*", distinct = false) : Int64
      query.count(column_name, distinct)
    end

    # Calculates the sum of a numeric column.
    #
    # Returns Int64 for integer columns, Float64 for decimal columns.
    #
    # ## Example
    #
    # ```
    # # Sum of all salaries
    # total = User.sum(:salary) # => 1500000
    #
    # # Sum with conditions
    # active_total = User.where(active: true).sum(:salary)
    # ```
    def sum(column_name : Symbol | String) : Int64 | Float64
      query.sum(column_name)
    end

    # Calculates the average of a numeric column.
    #
    # Always returns Float64.
    #
    # ## Example
    #
    # ```
    # # Average age
    # avg_age = User.average(:age) # => 32.5
    #
    # # Average with conditions
    # avg_salary = User.where(department: "Engineering").average(:salary)
    # ```
    def average(column_name : Symbol | String) : Float64
      query.average(column_name)
    end

    # Finds the minimum value in a column.
    #
    # ## Example
    #
    # ```
    # # Youngest user
    # min_age = User.minimum(:age) # => 18
    #
    # # Earliest created record
    # first_created = User.minimum(:created_at)
    # ```
    def minimum(column_name : Symbol | String)
      query.minimum(column_name)
    end

    # Finds the maximum value in a column.
    #
    # ## Example
    #
    # ```
    # # Oldest user
    # max_age = User.maximum(:age) # => 75
    #
    # # Most recent record
    # last_created = User.maximum(:created_at)
    # ```
    def maximum(column_name : Symbol | String)
      query.maximum(column_name)
    end

    # Specifies which columns to SELECT in the query.
    #
    # By default, all columns are selected (`SELECT *`). Use this to optimize
    # queries by only loading the columns you need.
    #
    # ## Example
    #
    # ```
    # # Select specific columns
    # users = User.select(:id, :name, :email).to_a
    #
    # # Chain with other methods
    # User.select(:id, :name).where(active: true).order(:name)
    # ```
    def select(*columns : Symbol) : Collection(self)
      query.select(*columns)
    end

    # Specifies a raw SQL SELECT clause.
    #
    # Useful for complex selections, aggregations, or PostgreSQL-specific functions.
    #
    # ## Example
    #
    # ```
    # # With SQL functions
    # User.select("id, UPPER(name) as name, LENGTH(email) as email_length")
    #
    # # With aggregations
    # User.select("department, COUNT(*) as employee_count").group_by(:department)
    # ```
    def select(sql : String) : Collection(self)
      query.select(sql)
    end

    # Adds DISTINCT to the query to remove duplicate rows.
    #
    # ## Example
    #
    # ```
    # # Get unique cities
    # cities = User.select(:city).distinct.pluck(:city)
    #
    # # Disable distinct
    # User.distinct(false)
    # ```
    def distinct(value = true) : Collection(self)
      query.distinct(value)
    end

    # Adds a WHERE clause with raw SQL.
    #
    # Use this for complex conditions that can't be expressed with the hash syntax.
    #
    # ## Example
    #
    # ```
    # # Simple raw SQL
    # User.where("age > 18")
    #
    # # With PostgreSQL functions
    # User.where("LENGTH(name) > 10")
    # ```
    def where(sql : String) : Collection(self)
      query.where(raw: sql)
    end

    # Adds WHERE conditions using a hash or named tuple.
    #
    # Supports equality, NULL checks, and IN queries with arrays.
    #
    # ## Example
    #
    # ```
    # # Equality
    # User.where({name: "John", active: true})
    #
    # # NULL check
    # User.where({deleted_at: nil})
    #
    # # IN query
    # User.where({id: [1, 2, 3, 4, 5]})
    #
    # # Named tuple
    # User.where({name: "John", age: 30})
    # ```
    def where(conditions : Hash(Symbol, Value | Array(Value)) | NamedTuple) : Collection(self)
      {% begin %}
      query.where(conditions).as(Collection({{@type}}))
      {% end %}
    end

    # Adds WHERE conditions using keyword arguments.
    #
    # Syntactic sugar for the hash-based where method.
    #
    # ## Example
    #
    # ```
    # User.where(name: "John", active: true)
    # User.where(age: 30, city: "NYC")
    # User.where(id: [1, 2, 3])
    # ```
    def where(**conditions) : Collection(self)
      query.where(**conditions)
    end

    # Adds WHERE clause with raw SQL and parameter binding.
    #
    # Use `?` as placeholders for parameters (converted to `$1`, `$2`, etc.).
    #
    # ## Example
    #
    # ```
    # # Single parameter
    # User.where("age > ?", 18)
    #
    # # Multiple parameters
    # User.where("age > ? AND city = ?", 18, "NYC")
    #
    # # With PostgreSQL functions
    # User.where("LENGTH(name) > ?", 10)
    # ```
    def where(sql : String, *splat : Value) : Collection(self)
      query.where(sql, *splat)
    end

    # Adds WHERE clause with raw SQL and an array of parameters.
    #
    # ## Example
    #
    # ```
    # params = [18, "NYC"]
    # User.where("age > ? AND city = ?", params)
    # ```
    def where(sql : String, args : Enumerable) : Collection(self)
      query.where(sql, args: args)
    end

    # Adds WHERE NOT conditions using a hash or named tuple.
    #
    # Negates the conditions - finds records that DON'T match.
    #
    # ## Example
    #
    # ```
    # # Not equal
    # User.where_not({status: "deleted"})
    #
    # # Not NULL
    # User.where_not({deleted_at: nil})
    #
    # # NOT IN
    # User.where_not({id: [1, 2, 3]})
    # ```
    def where_not(conditions : Hash(Symbol, Value | Array(Value)) | NamedTuple) : Collection(self)
      query.where_not(conditions)
    end

    # Adds WHERE NOT conditions using keyword arguments.
    #
    # ## Example
    #
    # ```
    # User.where_not(status: "deleted", banned: true)
    # ```
    def where_not(**conditions) : Collection(self)
      query.where_not(**conditions)
    end

    # Limits the number of records returned.
    #
    # ## Example
    #
    # ```
    # # Get first 10 users
    # User.limit(10).to_a
    #
    # # Combine with other methods
    # User.where(active: true).order(:name).limit(20)
    # ```
    def limit(value : Int32) : Collection(self)
      query.limit(value)
    end

    # Skips the specified number of records.
    #
    # Useful for pagination when combined with `limit`.
    #
    # ## Example
    #
    # ```
    # # Skip first 20 records
    # User.offset(20).limit(10).to_a
    #
    # # Page 3 (20 per page)
    # page = 3
    # per_page = 20
    # User.offset((page - 1) * per_page).limit(per_page)
    # ```
    def offset(value : Int32) : Collection(self)
      query.offset(value)
    end

    # Performs a SQL JOIN with another table.
    #
    # Supports LEFT, RIGHT, INNER, and FULL joins.
    #
    # ## Example
    #
    # ```
    # # Join with foreign key
    # User.join(:left, Group, :group_id)
    #
    # # Join with explicit primary key
    # User.join(:inner, Group, :group_id, Group)
    # ```
    def join(type : JoinType, model : Base.class, fk : Symbol, pk : Base.class | Nil = nil) : Collection(self)
      query.join(type, model, fk, pk)
    end

    # Performs a SQL JOIN with a custom ON clause.
    #
    # ## Example
    #
    # ```
    # # Custom join condition
    # User.join(:left, Group, "groups.id = users.group_id AND groups.active = true")
    # ```
    def join(type : JoinType, model : Base.class, on : String) : Collection(self)
      query.join(type, model, on)
    end

    # Groups results by one or more columns.
    #
    # Typically used with aggregate functions like COUNT, SUM, etc.
    #
    # ## Example
    #
    # ```
    # # Count users by city
    # User.select("city, COUNT(*) as count").group_by(:city)
    #
    # # Multiple columns
    # User.select("city, state, COUNT(*)").group_by(:city, :state)
    # ```
    def group_by(*columns : Symbol | String) : Collection(self)
      query.join(*columns)
    end

    # Orders results by one or more columns.
    #
    # Can specify direction (:asc or :desc) for each column.
    # Multiple calls to `order` are cumulative.
    #
    # ## Example
    #
    # ```
    # # Single column ascending (default)
    # User.order(:name)
    #
    # # Single column descending
    # User.order(created_at: :desc)
    #
    # # Multiple columns
    # User.order({name: :asc, created_at: :desc})
    # User.order(name: :asc, age: :desc)
    #
    # # Cumulative ordering
    # User.order(:name).order(:age) # ORDER BY name, age
    # ```
    def order(columns : Hash(Symbol, Symbol)) : Collection(self)
      query.order(columns)
    end

    # Orders results by column names (ascending by default).
    #
    # ## Example
    #
    # ```
    # User.order(:name, :age)
    # ```
    def order(*columns : Symbol | String) : Collection(self)
      query.order(*columns)
    end

    # Orders results using keyword arguments.
    #
    # ## Example
    #
    # ```
    # User.order(name: :asc, created_at: :desc)
    # ```
    def order(**columns) : Collection(self)
      query.order(**columns)
    end

    # Replaces any existing ORDER BY clause.
    #
    # Unlike `order` which is cumulative, `reorder` discards previous ordering.
    #
    # ## Example
    #
    # ```
    # # Original order is discarded
    # User.order(:name).reorder(:age) # ORDER BY age (not name, age)
    #
    # # Useful for overriding default scopes
    # User.order(:name).reorder(created_at: :desc)
    # ```
    def reorder(columns : Hash(Symbol, Symbol)) : Collection(self)
      query.reorder(columns)
    end

    # Replaces any existing ORDER BY clause with new columns.
    #
    # ## Example
    #
    # ```
    # User.order(:name).reorder(:age, :created_at)
    # ```
    def reorder(*columns : Symbol | String) : Collection(self)
      query.reorder(*columns)
    end

    # Replaces any existing ORDER BY clause using keyword arguments.
    #
    # ## Example
    #
    # ```
    # User.order(:name).reorder(age: :desc, created_at: :asc)
    # ```
    def reorder(**columns) : Collection(self)
      builder.reorder(**columns)
    end

    # Paginate results using page number (1-indexed)
    def paginate(page : Int32 = 1, limit : Int32 = 25) : PaginatedResult(self)
      query.paginate(page, limit)
    end

    # Paginate results using offset and limit directly
    def paginate_by_offset(offset : Int32 = 0, limit : Int32 = 25) : PaginatedResult(self)
      query.paginate_by_offset(offset, limit)
    end

    # Cursor-based pagination for efficient large dataset traversal
    def paginate_cursor(
      after : String? = nil,
      before : String? = nil,
      limit : Int32 = 25,
      cursor_column : Symbol = :id,
    ) : CursorPaginatedResult(self)
      query.paginate_cursor(after, before, limit, cursor_column)
    end

    # Pattern matching with LIKE operator
    def where_like(column : Symbol | String, pattern : String) : Collection(self)
      query.where_like(column, pattern)
    end

    # Pattern matching with ILIKE operator
    def where_ilike(column : Symbol | String, pattern : String) : Collection(self)
      query.where_ilike(column, pattern)
    end

    # Negated pattern matching with NOT LIKE
    def where_not_like(column : Symbol | String, pattern : String) : Collection(self)
      query.where_not_like(column, pattern)
    end

    # Negated pattern matching with NOT ILIKE
    def where_not_ilike(column : Symbol | String, pattern : String) : Collection(self)
      query.where_not_ilike(column, pattern)
    end

    # Greater than comparison
    def where_gt(column : Symbol | String, value : Value) : Collection(self)
      query.where_gt(column, value)
    end

    # Greater than or equal comparison
    def where_gte(column : Symbol | String, value : Value) : Collection(self)
      query.where_gte(column, value)
    end

    # Less than comparison
    def where_lt(column : Symbol | String, value : Value) : Collection(self)
      query.where_lt(column, value)
    end

    # Less than or equal comparison
    def where_lte(column : Symbol | String, value : Value) : Collection(self)
      query.where_lte(column, value)
    end

    # BETWEEN range comparison
    def where_between(column : Symbol | String, min : Value, max : Value) : Collection(self)
      query.where_between(column, min, max)
    end

    # NOT BETWEEN range comparison
    def where_not_between(column : Symbol | String, min : Value, max : Value) : Collection(self)
      query.where_not_between(column, min, max)
    end
  end
end

require "./builder"

module PgORM
  module Query
    module Methods(T)
      @builder : Builder?

      private def builder
        @builder.not_nil!
      end

      abstract def dup(builder : Builder) # : self

      # Ensures that the query will never return anything from the database.
      def none : self
        dup builder.none
      end

      # Query all records from the database. Doesn't actually issue any SQL query.
      # See `Cache#to_a` to load all records from the database into an Array.
      def all : self
        self
      end

      # Loads all primary key values of rows matching the SQL query.
      def ids : Array(T::PrimaryKeyType)
        if cache = @cache
          cache.map(&.id)
        else
          builder = self.builder.unscope(:select)
          builder.select!(T.primary_key)
          Database.adapter(builder).select_all(&.read(T::PrimaryKeyType))
        end
      end

      # Loads a record by id from the database. Raises a `Error::RecordNotFound`
      # exception when the record doesn't exist.
      #
      # ```
      # user = User.find(1)
      # # => SELECT * FROM "users" WHERE "id" = 1 LIMIT 1;
      # ```
      def find(id : T::PrimaryKeyType) : T
        find?(id) || raise Error::RecordNotFound.new
      end

      # Same as `#find` but returns `nil` when the record doesn't exist.
      def find?(id : T::PrimaryKeyType) : T?
        builder = self.builder.where({T.primary_key => id}).limit(1)
        Database.adapter(builder).select_one { |rs| T.new(rs) }
      end

      # Loads a record by arguments from the database. Raises a `RecordNotFound`
      # exception when the record doesn't exist. For example:
      #
      # ```
      # user = User.find_by(name: "user", group_id: 2)
      # # => SELECT * FROM "users" WHERE "name" = 'user' AND "group_id" = 2 LIMIT 1;
      # ```
      #
      # See `#where` for more details on conditions.
      def find_by(**args) : T
        find_by?(**args) || raise Error::RecordNotFound.new
      end

      # Same as `#find_by` but returns `nil` when no record could be found in the
      # database.
      def find_by?(**args) : T?
        builder = self.builder.where(**args).limit(1)
        Database.adapter(builder).select_one { |rs| T.new(rs) }
      end

      # Returns true if the SQL query has at least one result.
      #
      # ```
      # User.where(group_id: 1).exists?
      # # => SELECT 1 AS one FROM "users" WHERE "group_id" = 1 LIMIT 1;
      # ```
      def exists? : Bool
        builder = self.builder.unscope(:select, :order, :offset)
          .select!("1 AS one")
          .limit!(1)
        Database.adapter(builder).select_one { |_| true } || false
      end

      # Returns true when a record identified by primary key exists in the
      # database with the current conditions.
      #
      # ```
      # User.where(group_id: 1).exists?(2)
      # # => SELECT 1 AS one FROM "users" WHERE "group_id" = 1 AND "id" = 2 LIMIT 1;
      # ```
      def exists?(id : T::PrimaryKeyType) : Bool
        builder = self.builder.unscope(:select, :order, :offset)
          .select!("1 AS one")
          .where!({T.primary_key => id})
          .limit!(1)
        Database.adapter(builder).select_one { |_| true } || false
      end

      # Loads one record from the database, without any ordering. Raises a
      # `Error::RecordNotFound` exception when no record could be found.
      #
      # ```
      # user = User.take
      # # => SELECT * FROM "users" LIMIT 1;
      # ```
      def take : T
        take? || raise Error::RecordNotFound.new
      end

      # Same as `#take` but returns `nil` when no record could be found in the
      # database.
      def take? : T?
        if cache = @cache
          cache.first?
        else
          builder = self.builder.limit(1)
          Database.adapter(builder).select_one { |rs| T.new(rs) }
        end
      end

      # Loads the first record from the database, ordering by the primary key in
      # ascending order unless an order has been specified.
      #
      # Merely takes the last entry in the cached result set if the relation was
      # previously loaded.
      #
      # Prefer `#take` if you don't need an ordering or already specified one.
      #
      # ```
      # user = User.first
      # # => SELECT * FROM "users" ORDER BY "id" ASC LIMIT 1;
      #
      # user = User.order(name: :desc).last
      # # => SELECT * FROM "users" ORDER BY "name" DESC LIMIT 1;
      #
      # user = User.order("name ASC, group_id DESC").last
      # # => SELECT * FROM "users" ORDER BY name ASC, group_id DESC LIMIT 1;
      # ```
      def first : T
        first? || raise Error::RecordNotFound.new
      end

      # Same as `#first?` but returns `nil` when no record could be found in the
      # database.
      def first? : T?
        if cache = @cache
          cache.first?
        else
          builder = self.builder.limit(1)
          builder.order!({T.primary_key => :asc}) unless builder.orders?
          Database.adapter(builder).select_one { |rs| T.new(rs) }
        end
      end

      # Loads the last record from the database, ordering by the primary key in
      # ascending order unless an order has been specified.
      #
      # Merely takes the last entry in the cached result set if the relation was
      # previously loaded.
      #
      # Prefer `#take` if you don't need an ordering or already specified one.
      #
      # ```
      # user = User.last
      # # => SELECT * FROM "users" ORDER BY "id" DESC LIMIT 1;
      #
      # user = User.order(name: :desc).last
      # # => SELECT * FROM "users" ORDER BY "name" ASC LIMIT 1;
      #
      # user = User.order("name ASC, group_id DESC").last
      # # => SELECT * FROM "users" ORDER BY name DESC, group_id ASC LIMIT 1;
      # ```
      def last : T
        last? || raise Error::RecordNotFound.new
      end

      # Same as `#last?` but returns `nil` when no record could be found in the
      # database.
      def last? : T?
        if cache = @cache
          cache.last?
        else
          builder = self.builder.limit(1)
          if orders = builder.orders?
            builder.unscope!(:order)
            orders.each do |order|
              case order
              when Tuple
                builder.order!({order[0] => order[1] == :asc ? :desc : :asc})
              when String
                order = order.gsub(/\b(ASC|DESC)\b/i) do |m|
                  m.compare("ASC", case_insensitive: true) == 0 ? "DESC" : "ASC"
                end
                builder.order!(order)
              end
            end
          else
            builder.order!({T.primary_key => :desc})
          end
          Database.adapter(builder).select_one { |rs| T.new(rs) }
        end
      end

      # Loads values of a single column as an Array.
      #
      # ```
      # names = User.pluck(:name)
      # # => SELECT "name" FROM "users";
      # # => ["user", "alice", ...]
      # ```
      def pluck(column_name : Symbol | String) : Array(Value)
        builder = self.builder.unscope(:select)
        builder.select!(column_name)
        Database.adapter(builder).select_all(&.read(Value))
      end

      # Returns how many records match the SQL query. Uses the cached result set
      # if the query was previously loaded, otherwise executes a COUNT SQL query.
      def size : Int64
        if cache = @cache
          cache.size.to_i64
        else
          count
        end
      end

      # Counts how many records match the SQL query.
      #
      # You can count all columns or a specific column::
      # ```
      # User.count
      # User.count(:name)
      # ```
      #
      # You can specify a raw SQL query with a String:
      # ```
      # User.count("LENGTH(name)")
      # ```
      def count(column_name : Symbol | String = "*", distinct = builder.distinct?) : Int64
        calculate("COUNT", column_name, distinct).as(Int).to_i64
      end

      # Calculates the sum of a column.
      #
      # You can specify a column name:
      # ```
      # User.sum(:salary)
      # ```
      #
      # You can specify a raw SQL query with a String:
      # ```
      # User.sum("LENGTH(name)")
      # ```
      def sum(column_name : Symbol | String) : Int64 | Float64
        rs = calculate("SUM", column_name)
        if rs.responds_to?(:to_i64)
          rs.to_i64
        elsif rs.responds_to?(:to_f64)
          rs.to_f64
        elsif rs.nil?
          0_i64
        else
          raise Error.new("expected integer or floating point number but got #{rs.class.name}")
        end
      end

      # Calculates the average of a column. See `#sum` for details.
      def average(column_name : Symbol | String) : Float64
        rs = calculate("AVG", column_name)
        if rs.responds_to?(:to_f64)
          rs.to_f64
        elsif rs.nil?
          0.0
        else
          raise Error.new("expected floating point number but got #{rs.class.name}")
        end
      end

      # Returns the minimum value for a column. See `#sum` for details.
      def minimum(column_name : Symbol | String)
        calculate("MIN", column_name)
      end

      # Returns the maximum value for a column. See `#sum` for details.
      def maximum(column_name : Symbol | String)
        calculate("MAX", column_name)
      end

      protected def calculate(function, column_name, distinct = builder.distinct?)
        selects = String.build do |str|
          str << function
          str << '('
          str << "DISTINCT " if distinct
          case column_name
          when Symbol
            Database.quote(column_name, str)
          when String
            str << column_name
          end
          str << ')'
        end
        builder = self.builder.unscope(:select)
        builder.select!(selects)
        Database.adapter(builder).scalar
      end

      # Executes an UPDATE SQL query.
      # ```
      # User.where(id: 1).update_all({group_id: 2})
      # # => UPDATE "users" SET "group_id" = 2 WHERE "id" = 1;
      # ```
      def update_all(attributes : Hash | NamedTuple) : Nil
        Database.adapter(builder).update(attributes)
      end

      # Executes an UPDATE SQL query.
      # ```
      # User.where(id: 1).update_all(group_id: 2)
      # # => UPDATE "users" SET "group_id" = 2 WHERE "id" = 1;
      # ```
      def update_all(**attributes) : Nil
        update_all(attributes)
      end

      # Executes a DELETE SQL query.
      # ```
      # User.where(group_id: 1).delete_all
      # # => DELETE FROM "users" WHERE "group_id" = 1;
      # ```
      def delete_all : Nil
        Database.adapter(builder).delete
      end

      # Specify SELECT columns for the query.
      def select(*columns : Symbol) : self
        dup builder.select(*columns)
      end

      # :nodoc:
      def select!(*columns : Symbol) : self
        builder.select!(*columns)
        self
      end

      # Specify a raw SELECT statement for the query.
      def select(sql : String) : self
        dup builder.select(sql)
      end

      # :nodoc:
      def select!(sql : String) : self
        builder.select!(sql)
        self
      end

      # Specify a DISTINCT statement for the query.
      def distinct(value = true) : self
        dup builder.distinct(value)
      end

      # :nodoc:
      def distinct!(value = true) : self
        builder.distinct!(value)
      end

      # Specify WHERE conditions for the query. For example:
      # ```
      # conditions = {
      #   :name     => "user",
      #   :group_id => 2,
      # }
      # User.where(conditions)
      # # => SELECT * FROM "users" WHERE "name" = 'user' AND "group_id" = 2;
      # ```
      #
      # The condition value may be nil:
      # ```
      # User.where({:group_id => nil})
      # # => SELECT * FROM "users" WHERE "group_id" IS NULL;
      # ```
      #
      # The condition value may also be an Array of values:
      # ```
      # User.where({:group_id => [1, 2, 3]})
      # # => SELECT * FROM "users" WHERE "group_id" IN (1, 2, 3);
      # ```
      def where(conditions : Hash(Symbol, Value | Array(Value)) | NamedTuple) : self
        dup builder.where(conditions)
      end

      # :nodoc:
      def where!(conditions : Hash(Symbol, Value | Array(Value)) | NamedTuple) : self
        builder.where!(conditions)
        self
      end

      # Specify WHERE conditions for the query. For example:
      # ```
      # User.where(name: "user", group_id: 2)
      # # => SELECT * FROM "users" WHERE "name" = 'user' AND "group_id" = 2;
      # ```
      #
      # The condition value may be nil:
      # ```
      # User.where(group_id: nil)
      # # => SELECT * FROM "users" WHERE "group_id" IS NULL;
      # ```
      #
      # The condition value may also be an Array of values:
      # ```
      # User.where(group_id: [1, 2, 3])
      # # => SELECT * FROM "users" WHERE "group_id" IN (1, 2, 3);
      # ```
      def where(**conditions) : self
        dup builder.where(**conditions)
      end

      # :nodoc:
      def where!(**conditions) : self
        builder.where!(**conditions)
        self
      end

      def where_not(conditions : Hash(Symbol, Value | Array(Value)) | NamedTuple) : self
        dup builder.where_not(conditions)
      end

      # :nodoc:
      def where_not!(conditions : Hash(Symbol, Value | Array(Value)) | NamedTuple) : self
        builder.where_not!(conditions)
        self
      end

      def where_not(**conditions) : self
        dup builder.where_not(**conditions)
      end

      # :nodoc:
      def where_not!(**conditions) : self
        builder.where_not!(**conditions)
        self
      end

      # Specify a raw WHERE condition for the query. You can specify arguments as
      # `?` and pass them to the method. For example:
      #
      # ```
      # User.where("LENGTH(name) > ?", 10)
      # # => SELECT * FROM "users" WHERE LENGTH(name) > 10;
      # ```
      def where(sql : String, *args : Value) : self
        dup builder.where(sql, *args)
      end

      # :nodoc:
      def where!(sql : String, *args : Value) : self
        builder.where!(sql, *args)
        self
      end

      # Specify a LIMIT for the query.
      def limit(value : Int32) : self
        dup builder.limit(value)
      end

      # :nodoc:
      def limit!(value : Int32) : self
        builder.limit!(value)
        self
      end

      # Specify an OFFSET for the query.
      def offset(value : Int32) : self
        dup builder.offset(value)
      end

      # :nodoc:
      def offset!(value : Int32) : self
        builder.offset!(value)
        self
      end

      def join(model : Base.class, fk : Symbol) : self
        dup builder.join(model, fk)
      end

      def group_by(*columns : Symbol | String) : self
        dup builder.group_by(*columns)
      end

      # Specify an ORDER for the query. This is added to any previous order
      # definition. For example:
      # ```
      # User.order({name: :asc, group_id: :desc})
      # # => SELECT * FROM "users" ORDER BY "name" ASC, "group_id" DESC;
      # ```
      def order(columns : Hash(Symbol, Symbol)) : self
        dup builder.order(columns)
      end

      # :nodoc:
      def order!(columns : Hash(Symbol, Symbol)) : self
        builder.order!(columns)
        self
      end

      # Specify an ORDER column for the query. This is added to any previous order
      # definition. For example:
      # ```
      # User.order(:name)
      # # => SELECT * FROM "users" ORDER BY "name" ASC;
      # ```
      def order(*columns : Symbol | String) : self
        dup builder.order(*columns)
      end

      # :nodoc:
      def order!(*columns : Symbol | String) : self
        builder.order!(*columns)
        self
      end

      # Specify an ORDER for the query. This is added to any previous order
      # definition. For example:
      # ```
      # User.order(name: :asc, group_id: :desc)
      # # => SELECT * FROM "users" ORDER BY "name" ASC, "group_id" DESC;
      # ```
      def order(**columns) : self
        dup builder.order(**columns)
      end

      # :nodoc:
      def order!(**columns) : self
        builder.order!(**columns)
        self
      end

      # Specify an ORDER for the query, replacing any previous ORDER definition.
      # See `#order` for details.
      def reorder(columns : Hash(Symbol, Symbol)) : self
        dup builder.reorder(columns)
      end

      # :nodoc:
      def reorder!(columns : Hash(Symbol, Symbol)) : self
        builder.reorder!(columns)
        self
      end

      # Specify an ORDER column for the query, replacing any previous ORDER
      # definition. See `#order` for details.
      def reorder(*columns : Symbol | String) : self
        dup builder.reorder(*columns)
      end

      # :nodoc:
      def reorder!(*columns : Symbol | String) : self
        builder.reorder!(*columns)
        self
      end

      # Specify an ORDER for the query, replacing any previous ORDER definition.
      # See `#order` for details.
      def reorder(**columns) : self
        dup builder.reorder(**columns)
      end

      # :nodoc:
      def reorder!(**columns) : self
        builder.reorder!(**columns)
        self
      end

      # Resets previously set SQL statement(s). For example:
      #
      # ```
      # users = User.where(group_id: 1).limit(10)
      # users.unscope(:limit)         # == User.where(group_id: 1)
      # users.unscope(:where)         # == User.limit(10)
      # users.unscope(:where, :limit) # == User.all
      # ```
      #
      # Available properties:
      # - `:select`
      # - `:where`
      # - `:order`
      # - `:limit`
      # - `:offset`
      def unscope(*args) : self
        dup builder.unscope(*args)
      end

      # :nodoc:
      def unscope!(*args) : self
        builder.unscope!(*args)
        self
      end

      # Returns the generated SQL query. Useful for debugging.
      def to_sql : String
        Database.adapter(builder).to_sql
      end
    end
  end
end

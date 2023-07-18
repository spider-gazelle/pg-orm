require "./query/methods"
require "./query/cache"

module PgORM
  # Build Database Queries.
  #
  # A `Collection` is immutable: all methods will return a copy of the previous
  # `Collection` with the added constraint(s). For example:
  # ```
  # users = User.select(:id, :name).where(group_id: 2)
  # ```
  #
  # Termination methods such as `#find_by`, or `#take` will explicitly execute
  # a SQL request against the database and load one record.
  # ```
  # first_user = users.order(:name).take
  # # => SELECT "id", "name"
  # #    FROM "users"
  # #    WHERE "group_id" = 2
  # #    ORDER BY "name" ASC
  # #    LIMIT 1;
  # ```
  #
  # Termination methods such as `#to_a` or `#each` will execute a SQL request
  # then cache loaded records into the Collection, so further accesses won't
  # re-execute the SQL request. Some methods such as `#first` or `#size` will
  # leverage this cache when it's available.
  #
  # ```
  # users.to_a
  # # => SELECT "id", "name" FROM "users" WHERE "group_id" = 2;
  # ```
  #
  # When specifying column names you should always use a Symbol, so they'll be
  # properly quoted for the database server. In many cases you can specify raw
  # SQL statements using a String. For example:
  # ```
  # users = User.where("LENGTH(name) > $0", 10)
  # # => SELECT * FROM "users" WHERE LENGTH(name) > 10;
  #
  # users = User.order("LENGTH(name) DESC")
  # # => SELECT * FROM "users" ORDER BY LENGTH(name) DESC;
  #
  # count = User.count("LENGTH(name)", distinct: true)
  # # => SELECT COUNT(DISTINCT LENGTH(name)) FROM "users";
  # ```
  class Collection(T)
    include Enumerable(T)
    include Query::Methods(T)
    include Query::Cache(T)

    protected def initialize(@builder : Query::Builder)
    end

    protected def dup(builder : Query::Builder) : self
      Collection(T).new(builder)
    end

    # Yields a block with the chunks in the given size.
    #
    # ```
    # [1, 2, 4].in_groups_of(2, 0) { |e| p e.sum }
    # # => 3
    # # => 4
    # ```
    #
    # By default, a new array is created and yielded for each group.
    # * If *reuse* is given, the array can be reused
    # * If *reuse* is an `Array`, this array will be reused
    # * If *reuse* is truthy, the method will create a new array and reuse it.
    #
    # This can be used to prevent many memory allocations when each slice of
    # interest is to be used in a read-only fashion.
    def in_groups_of(size : Int, filled_up_with : U = nil, reuse = false, &) forall U
      raise ArgumentError.new("Size must be positive") if size <= 0
      offset = 0
      loop do
        slice = if reuse
                  unless reuse.is_a?(Array)
                    reuse = Array(T | U).new(size)
                  end
                  reuse.clear
                  reuse
                else
                  Array(T | U).new(size)
                end
        new_builder = builder.limit(size).offset(offset)
        Database.adapter(new_builder).select_each { |rs| slice << T.new(rs) }
        offset += size
        break if slice.size == 0
        (size - slice.size).times { slice << filled_up_with }
        yield slice
      end
    end
  end
end

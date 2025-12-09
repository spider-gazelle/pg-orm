require "./query/methods"
require "./query/cache"
require "./pagination"

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
    include Pagination(T)

    # Convenience method for full-text search
    def search(query : String, columns : Array(String), config : String = "english") : self
      search_query = FullTextSearch::SearchQuery.new(query, columns, config)
      search(search_query)
    end

    # Overload: Accepts Symbol columns
    def search(query : String, *columns : Symbol, config : String = "english") : self
      search(query, columns.to_a.map(&.to_s), config)
    end

    # Overload: Accepts String columns
    def search(query : String, *columns : String, config : String = "english") : self
      search(query, columns.to_a, config)
    end

    # Convenience method for weighted full-text search
    def search_weighted(query : String, weighted_columns : Hash(String, FullTextSearch::Weight), config : String = "english") : self
      columns = weighted_columns.keys
      weights = weighted_columns.map { |col, weight| FullTextSearch::WeightedColumn.new(col, weight) }
      search_query = FullTextSearch::SearchQuery.new(query, columns, config, weighted_columns: weights)
      search(search_query)
    end

    # Overload: Accepts Symbol keys
    def search_weighted(query : String, weighted_columns : Hash(Symbol, FullTextSearch::Weight), config : String = "english") : self
      search_weighted(query, weighted_columns.transform_keys(&.to_s), config)
    end

    # Convenience method for ranked full-text search
    def search_ranked(
      query : String,
      columns : Array(String),
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : FullTextSearch::RankFunction = FullTextSearch::RankFunction::Rank,
    ) : self
      search_query = FullTextSearch::SearchQuery.new(query, columns, config, rank_normalization, rank_function: rank_function)
      search_ranked(search_query)
    end

    # Overload: Accepts Symbol columns
    def search_ranked(
      query : String,
      *columns : Symbol,
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : FullTextSearch::RankFunction = FullTextSearch::RankFunction::Rank,
    ) : self
      search_ranked(query, columns.to_a.map(&.to_s), config, rank_normalization, rank_function)
    end

    # Overload: Accepts String columns
    def search_ranked(
      query : String,
      *columns : String,
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : FullTextSearch::RankFunction = FullTextSearch::RankFunction::Rank,
    ) : self
      search_ranked(query, columns.to_a, config, rank_normalization, rank_function)
    end

    # Convenience method for ranked weighted search
    def search_ranked_weighted(
      query : String,
      weighted_columns : Hash(String, FullTextSearch::Weight),
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : FullTextSearch::RankFunction = FullTextSearch::RankFunction::Rank,
    ) : self
      columns = weighted_columns.keys
      weights = weighted_columns.map { |col, weight| FullTextSearch::WeightedColumn.new(col, weight) }
      search_query = FullTextSearch::SearchQuery.new(query, columns, config, rank_normalization, weights, rank_function)
      search_ranked(search_query)
    end

    # Overload: Accepts Symbol keys
    def search_ranked_weighted(
      query : String,
      weighted_columns : Hash(Symbol, FullTextSearch::Weight),
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : FullTextSearch::RankFunction = FullTextSearch::RankFunction::Rank,
    ) : self
      search_ranked_weighted(query, weighted_columns.transform_keys(&.to_s), config, rank_normalization, rank_function)
    end

    # Convenience method for phrase search
    def search_phrase(phrase : String, columns : Array(String), config : String = "english") : self
      words = phrase.strip.split(/\s+/)
      tsquery = words.join(" <-> ")
      search_query = FullTextSearch::SearchQuery.new(tsquery, columns, config)
      search(search_query)
    end

    # Overload: Accepts Symbol columns
    def search_phrase(phrase : String, *columns : Symbol, config : String = "english") : self
      search_phrase(phrase, columns.to_a.map(&.to_s), config)
    end

    # Overload: Accepts String columns
    def search_phrase(phrase : String, *columns : String, config : String = "english") : self
      search_phrase(phrase, columns.to_a, config)
    end

    # Convenience method for proximity search
    def search_proximity(word1 : String, word2 : String, distance : Int32, columns : Array(String), config : String = "english") : self
      tsquery = "#{word1} <#{distance}> #{word2}"
      search_query = FullTextSearch::SearchQuery.new(tsquery, columns, config)
      search(search_query)
    end

    # Overload: Accepts Symbol columns
    def search_proximity(word1 : String, word2 : String, distance : Int32, *columns : Symbol, config : String = "english") : self
      search_proximity(word1, word2, distance, columns.to_a.map(&.to_s), config)
    end

    # Overload: Accepts String columns
    def search_proximity(word1 : String, word2 : String, distance : Int32, *columns : String, config : String = "english") : self
      search_proximity(word1, word2, distance, columns.to_a, config)
    end

    # Convenience method for prefix search
    def search_prefix(prefix : String, columns : Array(String), config : String = "english") : self
      tsquery = "#{prefix}:*"
      search_query = FullTextSearch::SearchQuery.new(tsquery, columns, config)
      search(search_query)
    end

    # Overload: Accepts Symbol columns
    def search_prefix(prefix : String, *columns : Symbol, config : String = "english") : self
      search_prefix(prefix, columns.to_a.map(&.to_s), config)
    end

    # Overload: Accepts String columns
    def search_prefix(prefix : String, *columns : String, config : String = "english") : self
      search_prefix(prefix, columns.to_a, config)
    end

    # Convenience method for plain text search
    def search_plain(text : String, columns : Array(String), config : String = "english") : self
      search_query = FullTextSearch::SearchQuery.new(text, columns, config, use_plain_query: true)
      search(search_query)
    end

    # Overload: Accepts Symbol columns
    def search_plain(text : String, *columns : Symbol, config : String = "english") : self
      search_plain(text, columns.to_a.map(&.to_s), config)
    end

    # Overload: Accepts String columns
    def search_plain(text : String, *columns : String, config : String = "english") : self
      search_plain(text, columns.to_a, config)
    end

    # Convenience method for searching pre-computed tsvector column
    def search_vector(query : String, vector_column : String, config : String = "english") : self
      raise ArgumentError.new("search query cannot be empty") if query.strip.empty?

      escaped_query = query.gsub("'", "''")
      where(raw: "#{PgORM::Database.quote(vector_column)} @@ to_tsquery('#{config}', '#{escaped_query}')")
    end

    # Overload: Accepts Symbol for vector column
    def search_vector(query : String, vector_column : Symbol, config : String = "english") : self
      search_vector(query, vector_column.to_s, config)
    end

    # Convenience method for searching pre-computed tsvector column with ranking
    def search_vector_ranked(
      query : String,
      vector_column : String,
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : FullTextSearch::RankFunction = FullTextSearch::RankFunction::Rank,
    ) : self
      raise ArgumentError.new("search query cannot be empty") if query.strip.empty?

      escaped_query = query.gsub("'", "''")

      rank_expr = "#{rank_function}(#{PgORM::Database.quote(vector_column)}, to_tsquery('#{config}', '#{escaped_query}')"
      rank_expr += ", #{rank_normalization}" if rank_normalization
      rank_expr += ") AS search_rank"

      table = builder.table_name
      select_sql = "#{table}.*, #{rank_expr}"
      self.select(select_sql)
        .where(raw: "#{PgORM::Database.quote(vector_column)} @@ to_tsquery('#{config}', '#{escaped_query}')")
        .order("search_rank DESC")
    end

    # Overload: Accepts Symbol for vector column
    def search_vector_ranked(
      query : String,
      vector_column : Symbol,
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : FullTextSearch::RankFunction = FullTextSearch::RankFunction::Rank,
    ) : self
      search_vector_ranked(query, vector_column.to_s, config, rank_normalization, rank_function)
    end

    # Convenience method for searching pre-computed tsvector column with plain text
    def search_vector_plain(text : String, vector_column : String, config : String = "english") : self
      raise ArgumentError.new("search text cannot be empty") if text.strip.empty?

      escaped_text = text.gsub("'", "''")
      where(raw: "#{PgORM::Database.quote(vector_column)} @@ plainto_tsquery('#{config}', '#{escaped_text}')")
    end

    # Overload: Accepts Symbol for vector column
    def search_vector_plain(text : String, vector_column : Symbol, config : String = "english") : self
      search_vector_plain(text, vector_column.to_s, config)
    end

    # Returns the query execution plan (useful for optimization)
    # Note: This only works for SELECT queries (Collection is for queries, not mutations)
    def explain : String
      adapter = PgORM::Database.adapter(builder)
      sql, args = adapter.select_sql
      explain_sql = "EXPLAIN ANALYZE #{sql}"

      result = [] of String
      PgORM::Database.connection do |db|
        db.query(explain_sql, args: args) do |rs|
          rs.each do
            result << rs.read(String)
          end
        end
      end
      result.join("\n")
    end

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

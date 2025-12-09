require "json"
require "yaml"
require "./query/methods"
require "./query/cache"
require "./pagination"

module PgORM
  class Relation(T)
    include JSON::Serializable
    include YAML::Serializable
    include Enumerable(T)
    include Query::Methods(T)
    include Query::Cache(T)
    include Pagination(T)

    # :nodoc:
    def initialize(@parent : Base, @foreign_key : Symbol, @builder : Query::Builder? = nil)
    end

    def build(**attributes) : T
      record = T.new(**attributes)
      record[@foreign_key] = @parent.id?
      (@cache ||= [] of T) << record
      record
    end

    def create(**attributes) : T
      record = build(**attributes)
      record.save
      record
    end

    def delete(*records : T) : Nil
      if T.primary_key.is_a?(Tuple)
        records.each(&.delete)
      else
        ids = records.map(&.id)
        where({T.primary_key.as(Symbol) => ids.to_a}).delete_all
        @cache.try(&.reject! { |r| ids.includes?(r.id) })
      end
    end

    protected def dup(builder : Query::Builder) : self
      Relation(T).new(@parent, @foreign_key, builder)
    end

    protected def builder
      @builder ||=
        if (id = @parent.id?) && id.is_a?(Value)
          builder = Query::Builder.new(T.table_name, T.primary_key.to_s)
          builder.where!({@foreign_key => id})
          builder
        else
          raise Error::RecordNotSaved.new("can't initialize Relation(#{T.name}) for #{@parent.class.name} doesn't have an id.")
        end
    end

    # :nodoc:
    def parent
      @parent
    end

    # Convenience method for full-text search
    def search(query : String, columns : Array(String), config : String = "english") : self
      search_query = FullTextSearch::SearchQuery.new(query, columns, config)
      search(search_query)
    end

    def search(query : String, *columns : Symbol, config : String = "english") : self
      search(query, columns.to_a.map(&.to_s), config)
    end

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

    def search_ranked(
      query : String,
      *columns : Symbol,
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : FullTextSearch::RankFunction = FullTextSearch::RankFunction::Rank,
    ) : self
      search_ranked(query, columns.to_a.map(&.to_s), config, rank_normalization, rank_function)
    end

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

    def search_phrase(phrase : String, *columns : Symbol, config : String = "english") : self
      search_phrase(phrase, columns.to_a.map(&.to_s), config)
    end

    def search_phrase(phrase : String, *columns : String, config : String = "english") : self
      search_phrase(phrase, columns.to_a, config)
    end

    # Convenience method for proximity search
    def search_proximity(word1 : String, word2 : String, distance : Int32, columns : Array(String), config : String = "english") : self
      tsquery = "#{word1} <#{distance}> #{word2}"
      search_query = FullTextSearch::SearchQuery.new(tsquery, columns, config)
      search(search_query)
    end

    def search_proximity(word1 : String, word2 : String, distance : Int32, *columns : Symbol, config : String = "english") : self
      search_proximity(word1, word2, distance, columns.to_a.map(&.to_s), config)
    end

    def search_proximity(word1 : String, word2 : String, distance : Int32, *columns : String, config : String = "english") : self
      search_proximity(word1, word2, distance, columns.to_a, config)
    end

    # Convenience method for prefix search
    def search_prefix(prefix : String, columns : Array(String), config : String = "english") : self
      tsquery = "#{prefix}:*"
      search_query = FullTextSearch::SearchQuery.new(tsquery, columns, config)
      search(search_query)
    end

    def search_prefix(prefix : String, *columns : Symbol, config : String = "english") : self
      search_prefix(prefix, columns.to_a.map(&.to_s), config)
    end

    def search_prefix(prefix : String, *columns : String, config : String = "english") : self
      search_prefix(prefix, columns.to_a, config)
    end

    # Convenience method for plain text search
    def search_plain(text : String, columns : Array(String), config : String = "english") : self
      search_query = FullTextSearch::SearchQuery.new(text, columns, config, use_plain_query: true)
      search(search_query)
    end

    def search_plain(text : String, *columns : Symbol, config : String = "english") : self
      search_plain(text, columns.to_a.map(&.to_s), config)
    end

    def search_plain(text : String, *columns : String, config : String = "english") : self
      search_plain(text, columns.to_a, config)
    end

    # Convenience method for searching pre-computed tsvector column
    def search_vector(query : String, vector_column : String, config : String = "english") : self
      raise ArgumentError.new("search query cannot be empty") if query.strip.empty?

      escaped_query = query.gsub("'", "''")
      where(raw: "#{PgORM::Database.quote(vector_column)} @@ to_tsquery('#{config}', '#{escaped_query}')")
    end

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

      table = @parent.class.table_name
      select_sql = "#{table}.*, #{rank_expr}"
      self.select(select_sql)
        .where(raw: "#{PgORM::Database.quote(vector_column)} @@ to_tsquery('#{config}', '#{escaped_query}')")
        .order("search_rank DESC")
    end

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

    def search_vector_plain(text : String, vector_column : Symbol, config : String = "english") : self
      search_vector_plain(text, vector_column.to_s, config)
    end

    delegate to_json, to_pretty_json, from_json, to: @parent
    delegate to_yaml, from_yaml, to: @parent
  end
end

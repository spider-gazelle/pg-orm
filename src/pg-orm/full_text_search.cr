module PgORM
  # Full-text search support using PostgreSQL's tsvector and tsquery.
  #
  # This module provides a comprehensive interface to PostgreSQL's powerful full-text
  # search capabilities, including:
  # - Basic text search with multiple columns
  # - Weighted search (prioritize certain columns)
  # - Ranked search (order by relevance)
  # - Phrase search (exact phrase matching)
  # - Proximity search (words within N positions)
  # - Prefix search (word prefix matching)
  # - Pre-computed tsvector columns (for production performance)
  #
  # ## Basic Usage
  #
  # ```
  # # Simple search across multiple columns
  # Article.search("crystal programming", :title, :content)
  #
  # # Weighted search (title more important than content)
  # Article.search_weighted("crystal", {
  #   title:   FullTextSearch::Weight::A, # Weight 1.0
  #   content: FullTextSearch::Weight::B, # Weight 0.4
  # })
  #
  # # Ranked search (ordered by relevance)
  # Article.search_ranked("crystal programming", :title, :content)
  # ```
  #
  # ## Advanced Usage
  #
  # ```
  # # Phrase search (exact phrase)
  # Article.search_phrase("crystal programming language", :content)
  #
  # # Proximity search (words within 5 positions)
  # Article.search_proximity("crystal", "programming", 5, :content)
  #
  # # Prefix search (matches crystal, crystalline, etc.)
  # Article.search_prefix("cryst", :title, :content)
  # ```
  #
  # ## Production Optimization
  #
  # For better performance, use pre-computed tsvector columns:
  #
  # ```sql
  # -- Add tsvector column
  # ALTER TABLE articles ADD COLUMN search_vector tsvector;
  #
  # -- Create GIN index
  # CREATE INDEX articles_search_idx ON articles USING GIN(search_vector);
  #
  # -- Auto-update trigger
  # CREATE TRIGGER articles_search_update
  #   BEFORE INSERT OR UPDATE ON articles
  #   FOR EACH ROW EXECUTE FUNCTION
  #   tsvector_update_trigger(search_vector, 'pg_catalog.english', title, content);
  # ```
  #
  # Then use the optimized search methods:
  #
  # ```
  # Article.search_vector("crystal programming", :search_vector)
  # Article.search_vector_ranked("crystal", :search_vector)
  # ```
  module FullTextSearch
    # Column weights for full-text search ranking.
    #
    # PostgreSQL assigns different weights to text based on importance:
    # - **A**: Weight 1.0 (highest priority, e.g., titles)
    # - **B**: Weight 0.4 (high priority, e.g., headings)
    # - **C**: Weight 0.2 (medium priority, e.g., abstracts)
    # - **D**: Weight 0.1 (lowest priority, e.g., body text)
    #
    # ## Example
    #
    # ```
    # Article.search_weighted("crystal", {
    #   title:    FullTextSearch::Weight::A, # Most important
    #   subtitle: FullTextSearch::Weight::B,
    #   content:  FullTextSearch::Weight::D, # Least important
    # })
    # ```
    enum Weight
      A
      B
      C
      D

      def to_char : Char
        case self
        when .a? then 'A'
        when .b? then 'B'
        when .c? then 'C'
        when .d? then 'D'
        else          'D'
        end
      end
    end

    # Represents a weighted column for full-text search
    struct WeightedColumn
      getter column : String
      getter weight : Weight

      def initialize(@column : String, @weight : Weight = Weight::D)
      end
    end

    # Ranking function type for relevance scoring.
    #
    # PostgreSQL provides two ranking functions:
    # - **Rank**: Standard ranking based on term frequency (`ts_rank`)
    # - **RankCD**: Cover density ranking, considers proximity of terms (`ts_rank_cd`)
    #
    # Cover density ranking often produces better results for phrase searches
    # and when term proximity matters.
    #
    # ## Example
    #
    # ```
    # # Standard ranking
    # Article.search_ranked("crystal", :content, rank_function: RankFunction::Rank)
    #
    # # Cover density ranking (better for phrases)
    # Article.search_ranked("crystal programming", :content, rank_function: RankFunction::RankCD)
    # ```
    enum RankFunction
      Rank   # ts_rank
      RankCD # ts_rank_cd (cover density)

      def to_s : String
        case self
        when .rank?    then "ts_rank"
        when .rank_cd? then "ts_rank_cd"
        else                "ts_rank"
        end
      end
    end

    # Represents a full-text search query configuration
    struct SearchQuery
      getter query : String
      getter config : String
      getter columns : Array(String)
      getter weighted_columns : Array(WeightedColumn)?
      getter rank_normalization : Int32?
      getter rank_function : RankFunction
      getter? use_plain_query : Bool

      def initialize(
        @query : String,
        @columns : Array(String),
        @config : String = "english",
        @rank_normalization : Int32? = nil,
        @weighted_columns : Array(WeightedColumn)? = nil,
        @rank_function : RankFunction = RankFunction::Rank,
        @use_plain_query : Bool = false,
      )
      end
    end

    # Performs full-text search using to_tsvector and to_tsquery
    #
    # ```
    # Article.search("crystal & programming", :title, :content)
    # Article.search("crystal | ruby", :title, config: "simple")
    # Article.search("cryst:*", :title)               # Prefix matching
    # Article.search("crystal", ["title", "content"]) # Array of strings
    # ```
    def search(query : String, columns : Array(String), config : String = "english") : Collection(self)
      raise ArgumentError.new("search query cannot be empty") if query.strip.empty?
      raise ArgumentError.new("at least one column must be specified") if columns.empty?

      search_query = SearchQuery.new(query, columns, config)
      self.query.search(search_query)
    end

    # Overload: Accepts Symbol columns
    def search(query : String, *columns : Symbol, config : String = "english") : Collection(self)
      search(query, columns.to_a.map(&.to_s), config)
    end

    # Overload: Accepts String columns
    def search(query : String, *columns : String, config : String = "english") : Collection(self)
      search(query, columns.to_a, config)
    end

    # Performs full-text search with weighted columns
    #
    # ```
    # Article.search_weighted("crystal", {"title" => Weight::A, "content" => Weight::B})
    # Article.search_weighted("crystal", {title: Weight::A, content: Weight::B})
    # ```
    def search_weighted(query : String, weighted_columns : Hash(String, Weight), config : String = "english") : Collection(self)
      raise ArgumentError.new("search query cannot be empty") if query.strip.empty?
      raise ArgumentError.new("at least one column must be specified") if weighted_columns.empty?

      columns = weighted_columns.keys
      weights = weighted_columns.map { |col, weight| WeightedColumn.new(col, weight) }
      search_query = SearchQuery.new(query, columns, config, weighted_columns: weights)
      self.query.search(search_query)
    end

    # Overload: Accepts Symbol keys for weighted columns
    def search_weighted(query : String, weighted_columns : Hash(Symbol, Weight), config : String = "english") : Collection(self)
      search_weighted(query, weighted_columns.transform_keys(&.to_s), config)
    end

    # Performs full-text search and orders by relevance rank
    #
    # ```
    # Article.search_ranked("crystal programming", ["title", "content"])
    # Article.search_ranked("ruby", ["title", "content"], rank_normalization: 1)
    # Article.search_ranked("ruby", :title, :content, rank_function: RankFunction::RankCD)
    # ```
    def search_ranked(
      query : String,
      columns : Array(String),
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : RankFunction = RankFunction::Rank,
    ) : Collection(self)
      raise ArgumentError.new("search query cannot be empty") if query.strip.empty?
      raise ArgumentError.new("at least one column must be specified") if columns.empty?

      search_query = SearchQuery.new(
        query,
        columns,
        config,
        rank_normalization,
        rank_function: rank_function
      )
      self.query.search_ranked(search_query)
    end

    # Overload: Accepts Symbol columns
    def search_ranked(
      query : String,
      *columns : Symbol,
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : RankFunction = RankFunction::Rank,
    ) : Collection(self)
      search_ranked(query, columns.to_a.map(&.to_s), config, rank_normalization, rank_function)
    end

    # Overload: Accepts String columns
    def search_ranked(
      query : String,
      *columns : String,
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : RankFunction = RankFunction::Rank,
    ) : Collection(self)
      search_ranked(query, columns.to_a, config, rank_normalization, rank_function)
    end

    # Performs full-text search with weighted columns and ranking
    #
    # ```
    # Article.search_ranked_weighted("crystal", {"title" => Weight::A, "content" => Weight::B})
    # Article.search_ranked_weighted("crystal", {title: Weight::A, content: Weight::B})
    # ```
    def search_ranked_weighted(
      query : String,
      weighted_columns : Hash(String, Weight),
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : RankFunction = RankFunction::Rank,
    ) : Collection(self)
      raise ArgumentError.new("search query cannot be empty") if query.strip.empty?
      raise ArgumentError.new("at least one column must be specified") if weighted_columns.empty?

      columns = weighted_columns.keys
      weights = weighted_columns.map { |col, weight| WeightedColumn.new(col, weight) }
      search_query = SearchQuery.new(
        query,
        columns,
        config,
        rank_normalization,
        weights,
        rank_function
      )
      self.query.search_ranked(search_query)
    end

    # Overload: Accepts Symbol keys for weighted columns
    def search_ranked_weighted(
      query : String,
      weighted_columns : Hash(Symbol, Weight),
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : RankFunction = RankFunction::Rank,
    ) : Collection(self)
      search_ranked_weighted(query, weighted_columns.transform_keys(&.to_s), config, rank_normalization, rank_function)
    end

    # Performs phrase search (exact phrase matching)
    #
    # ```
    # Article.search_phrase("crystal programming language", ["content"])
    # Article.search_phrase("crystal programming", :title, :content)
    # ```
    def search_phrase(phrase : String, columns : Array(String), config : String = "english") : Collection(self)
      raise ArgumentError.new("search phrase cannot be empty") if phrase.strip.empty?
      raise ArgumentError.new("at least one column must be specified") if columns.empty?

      # Convert phrase to tsquery format: "word1 <-> word2 <-> word3"
      words = phrase.strip.split(/\s+/)
      tsquery = words.join(" <-> ")

      search_query = SearchQuery.new(tsquery, columns, config)
      self.query.search(search_query)
    end

    # Overload: Accepts Symbol columns
    def search_phrase(phrase : String, *columns : Symbol, config : String = "english") : Collection(self)
      search_phrase(phrase, columns.to_a.map(&.to_s), config)
    end

    # Overload: Accepts String columns
    def search_phrase(phrase : String, *columns : String, config : String = "english") : Collection(self)
      search_phrase(phrase, columns.to_a, config)
    end

    # Performs proximity search (words within N positions)
    #
    # ```
    # Article.search_proximity("crystal", "programming", 5, :content) # Within 5 words
    # ```
    def search_proximity(word1 : String, word2 : String, distance : Int32, columns : Array(String), config : String = "english") : Collection(self)
      raise ArgumentError.new("words cannot be empty") if word1.strip.empty? || word2.strip.empty?
      raise ArgumentError.new("at least one column must be specified") if columns.empty?

      # Format: word1 <distance> word2
      tsquery = "#{word1} <#{distance}> #{word2}"

      search_query = SearchQuery.new(tsquery, columns, config)
      self.query.search(search_query)
    end

    # Overload: Accepts Symbol columns
    def search_proximity(word1 : String, word2 : String, distance : Int32, *columns : Symbol, config : String = "english") : Collection(self)
      search_proximity(word1, word2, distance, columns.to_a.map(&.to_s), config)
    end

    # Overload: Accepts String columns
    def search_proximity(word1 : String, word2 : String, distance : Int32, *columns : String, config : String = "english") : Collection(self)
      search_proximity(word1, word2, distance, columns.to_a, config)
    end

    # Performs prefix search
    #
    # ```
    # Article.search_prefix("cryst", :title, :content) # Matches crystal, crystalline, etc.
    # ```
    def search_prefix(prefix : String, columns : Array(String), config : String = "english") : Collection(self)
      raise ArgumentError.new("prefix cannot be empty") if prefix.strip.empty?
      raise ArgumentError.new("at least one column must be specified") if columns.empty?

      # Add :* for prefix matching
      tsquery = "#{prefix}:*"

      search_query = SearchQuery.new(tsquery, columns, config)
      self.query.search(search_query)
    end

    # Overload: Accepts Symbol columns
    def search_prefix(prefix : String, *columns : Symbol, config : String = "english") : Collection(self)
      search_prefix(prefix, columns.to_a.map(&.to_s), config)
    end

    # Overload: Accepts String columns
    def search_prefix(prefix : String, *columns : String, config : String = "english") : Collection(self)
      search_prefix(prefix, columns.to_a, config)
    end

    # Performs plain text search (automatically converts to tsquery)
    #
    # ```
    # Article.search_plain("crystal programming", :title, :content)
    # ```
    def search_plain(text : String, columns : Array(String), config : String = "english") : Collection(self)
      raise ArgumentError.new("search text cannot be empty") if text.strip.empty?
      raise ArgumentError.new("at least one column must be specified") if columns.empty?

      search_query = SearchQuery.new(text, columns, config, use_plain_query: true)
      self.query.search(search_query)
    end

    # Overload: Accepts Symbol columns
    def search_plain(text : String, *columns : Symbol, config : String = "english") : Collection(self)
      search_plain(text, columns.to_a.map(&.to_s), config)
    end

    # Overload: Accepts String columns
    def search_plain(text : String, *columns : String, config : String = "english") : Collection(self)
      search_plain(text, columns.to_a, config)
    end

    # Searches using a pre-computed tsvector column (recommended for production)
    #
    # ```
    # # Setup (run once):
    # # ALTER TABLE articles ADD COLUMN search_vector tsvector;
    # # CREATE INDEX articles_search_idx ON articles USING GIN(search_vector);
    # # CREATE TRIGGER articles_search_update BEFORE INSERT OR UPDATE ON articles
    # #   FOR EACH ROW EXECUTE FUNCTION
    # #   tsvector_update_trigger(search_vector, 'pg_catalog.english', title, content);
    #
    # Article.search_vector("crystal & programming", "search_vector")
    # Article.search_vector("crystal | ruby", :search_vector, config: "simple")
    # ```
    def search_vector(query : String, vector_column : String, config : String = "english") : Collection(self)
      raise ArgumentError.new("search query cannot be empty") if query.strip.empty?

      escaped_query = query.gsub("'", "''")
      self.query.where(raw: "#{Database.quote(vector_column)} @@ to_tsquery('#{config}', '#{escaped_query}')")
    end

    # Overload: Accepts Symbol for vector column
    def search_vector(query : String, vector_column : Symbol, config : String = "english") : Collection(self)
      search_vector(query, vector_column.to_s, config)
    end

    # Searches using a pre-computed tsvector column with ranking
    #
    # ```
    # Article.search_vector_ranked("crystal", "search_vector")
    # Article.search_vector_ranked("crystal", :search_vector, rank_normalization: 1)
    # Article.search_vector_ranked("crystal", :search_vector, rank_function: RankFunction::RankCD)
    # ```
    def search_vector_ranked(
      query : String,
      vector_column : String,
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : RankFunction = RankFunction::Rank,
    ) : Collection(self)
      raise ArgumentError.new("search query cannot be empty") if query.strip.empty?

      escaped_query = query.gsub("'", "''")

      # Build rank expression
      rank_expr = "#{rank_function}(#{Database.quote(vector_column)}, to_tsquery('#{config}', '#{escaped_query}')"
      rank_expr += ", #{rank_normalization}" if rank_normalization
      rank_expr += ") AS search_rank"

      self.query
        .select("#{table_name}.*, #{rank_expr}")
        .where(raw: "#{Database.quote(vector_column)} @@ to_tsquery('#{config}', '#{escaped_query}')")
        .order("search_rank DESC")
    end

    # Overload: Accepts Symbol for vector column
    def search_vector_ranked(
      query : String,
      vector_column : Symbol,
      config : String = "english",
      rank_normalization : Int32? = nil,
      rank_function : RankFunction = RankFunction::Rank,
    ) : Collection(self)
      search_vector_ranked(query, vector_column.to_s, config, rank_normalization, rank_function)
    end

    # Searches using a pre-computed tsvector column with plain text query
    #
    # ```
    # Article.search_vector_plain("crystal programming", "search_vector")
    # Article.search_vector_plain("crystal programming", :search_vector)
    # ```
    def search_vector_plain(text : String, vector_column : String, config : String = "english") : Collection(self)
      raise ArgumentError.new("search text cannot be empty") if text.strip.empty?

      escaped_text = text.gsub("'", "''")
      self.query.where(raw: "#{Database.quote(vector_column)} @@ plainto_tsquery('#{config}', '#{escaped_text}')")
    end

    # Overload: Accepts Symbol for vector column
    def search_vector_plain(text : String, vector_column : Symbol, config : String = "english") : Collection(self)
      search_vector_plain(text, vector_column.to_s, config)
    end
  end
end

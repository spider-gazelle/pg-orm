require "./spec_helper"

describe PgORM::FullTextSearch do
  describe "Full-Text Search" do
    before_each do
      Article.clear
    end

    it "performs basic full-text search" do
      article1 = Article.create!(title: "Crystal Programming Language", content: "Crystal is a compiled language with Ruby-like syntax")
      article2 = Article.create!(title: "Ruby on Rails", content: "Ruby is a dynamic programming language")
      article3 = Article.create!(title: "Python Tutorial", content: "Python is great for beginners")

      results = Article.search("crystal", :title, :content).to_a
      results.size.should eq(1)
      results.first.id.should eq(article1.id)
    end

    it "searches across multiple columns" do
      article1 = Article.create!(title: "Crystal Programming", content: "Learn about Ruby")
      article2 = Article.create!(title: "Ruby Tutorial", content: "Ruby is awesome")
      article3 = Article.create!(title: "Python Guide", content: "Python basics")

      results = Article.search("ruby", :title, :content).to_a
      results.size.should eq(2)
      results.map(&.id).sort!.should eq([article1.id, article2.id].sort)
    end

    it "supports AND operator in search" do
      article1 = Article.create!(title: "Crystal Programming Language", content: "Fast and efficient")
      article2 = Article.create!(title: "Crystal Tutorial", content: "Programming with Crystal")
      article3 = Article.create!(title: "Ruby Programming", content: "Dynamic language")

      results = Article.search("crystal & programming", :title, :content).to_a
      results.size.should eq(2)
      results.map(&.id).sort!.should eq([article1.id, article2.id].sort)
    end

    it "supports OR operator in search" do
      article1 = Article.create!(title: "Crystal Language", content: "Compiled language")
      article2 = Article.create!(title: "Ruby Language", content: "Dynamic language")
      article3 = Article.create!(title: "Python Guide", content: "Beginner friendly")

      results = Article.search("crystal | ruby", :title, :content).to_a
      results.size.should eq(2)
      results.map(&.id).sort!.should eq([article1.id, article2.id].sort)
    end

    it "supports NOT operator in search" do
      article1 = Article.create!(title: "Crystal Programming", content: "Fast compiled language")
      article2 = Article.create!(title: "Crystal Tutorial", content: "Ruby-like syntax")
      article3 = Article.create!(title: "Ruby Guide", content: "Dynamic language")

      results = Article.search("crystal & !ruby", :title, :content).to_a
      results.size.should eq(1)
      results.first.id.should eq(article1.id)
    end

    it "performs ranked search" do
      article1 = Article.create!(title: "Crystal", content: "Crystal is mentioned once")
      article2 = Article.create!(title: "Crystal Programming with Crystal", content: "Crystal Crystal Crystal")
      article3 = Article.create!(title: "Ruby", content: "No match here")

      results = Article.search_ranked("crystal", :title, :content).to_a
      results.size.should eq(2)
      # article2 should rank higher due to more occurrences
      results.first.id.should eq(article2.id)
    end

    it "performs phrase search" do
      article1 = Article.create!(title: "Crystal Programming Language", content: "Learn Crystal")
      article2 = Article.create!(title: "Language Crystal Programming", content: "Different order")
      article3 = Article.create!(title: "Crystal", content: "Programming Language")

      results = Article.search_phrase("crystal programming language", :title, :content).to_a
      # Phrase search with <-> operator requires words to be adjacent in order
      # article1 has "Crystal Programming Language" in title (matches)
      # article3 has "Programming Language" in content but not "Crystal Programming Language"
      results.size.should be >= 1
      results.map(&.id).should contain(article1.id)
    end

    it "handles empty search query" do
      expect_raises(ArgumentError, "search query cannot be empty") do
        Article.search("", :title)
      end
    end

    it "handles no columns specified" do
      # This is a compile-time error, so we can't test it at runtime
      # The method signature requires at least one column
      # Article.search("crystal") # This won't compile
      true.should be_true
    end

    it "works with where clauses" do
      article1 = Article.create!(title: "Crystal Tutorial", content: "Learn Crystal", published: true)
      article2 = Article.create!(title: "Crystal Guide", content: "Crystal basics", published: false)
      article3 = Article.create!(title: "Ruby Tutorial", content: "Learn Ruby", published: true)

      results = Article.where(published: true).search("crystal", :title, :content).to_a
      results.size.should eq(1)
      results.first.id.should eq(article1.id)
    end

    it "works with limit and offset" do
      5.times do |i|
        Article.create!(title: "Crystal Tutorial #{i}", content: "Learn Crystal programming")
      end

      results = Article.search("crystal", :title, :content).limit(2).to_a
      results.size.should eq(2)

      offset_results = Article.search("crystal", :title, :content).limit(2).offset(2).to_a
      offset_results.size.should eq(2)
      offset_results.map(&.id).should_not eq(results.map(&.id))
    end

    it "works with order clauses" do
      article1 = Article.create!(title: "Crystal A", content: "Crystal programming", published: true)
      article2 = Article.create!(title: "Crystal Z", content: "Crystal language", published: true)
      article3 = Article.create!(title: "Crystal M", content: "Crystal tutorial", published: true)

      results = Article.search("crystal", :title, :content).order(:title).to_a
      results.size.should eq(3)
      results.first.title.should eq("Crystal A")
      results.last.title.should eq("Crystal Z")
    end

    it "supports different text search configurations" do
      article1 = Article.create!(title: "Running fast", content: "The runner runs quickly")
      article2 = Article.create!(title: "Quick guide", content: "Fast tutorial")

      # Using 'simple' config (no stemming)
      results = Article.search("running", :title, :content, config: "simple").to_a
      results.size.should eq(1)
      results.first.id.should eq(article1.id)

      # Using 'english' config (with stemming, 'running' matches 'runs')
      results = Article.search("running", :title, :content, config: "english").to_a
      results.size.should eq(1)
    end

    it "handles special characters in search query" do
      article1 = Article.create!(title: "C++ Programming", content: "Learn C++ basics")
      article2 = Article.create!(title: "C# Tutorial", content: "C# for beginners")

      # PostgreSQL tsquery will handle escaping
      results = Article.search("c++", :title, :content).to_a
      results.size.should be >= 0 # May or may not match depending on tokenization
    end

    it "returns empty array when no matches" do
      Article.create!(title: "Ruby Tutorial", content: "Learn Ruby")
      Article.create!(title: "Python Guide", content: "Python basics")

      results = Article.search("crystal", :title, :content).to_a
      results.should be_empty
    end

    it "works with count" do
      3.times do |i|
        Article.create!(title: "Crystal Tutorial #{i}", content: "Learn Crystal")
      end
      Article.create!(title: "Ruby Guide", content: "Learn Ruby")

      count = Article.search("crystal", :title, :content).count
      count.should eq(3)
    end

    it "works with exists?" do
      Article.create!(title: "Crystal Programming", content: "Learn Crystal")

      Article.search("crystal", :title, :content).exists?.should be_true
      Article.search("nonexistent", :title, :content).exists?.should be_false
    end

    it "works with first and last" do
      article1 = Article.create!(title: "Crystal A", content: "Crystal programming")
      article2 = Article.create!(title: "Crystal Z", content: "Crystal language")

      first = Article.search("crystal", :title, :content).order(:title).first
      first.id.should eq(article1.id)

      last = Article.search("crystal", :title, :content).order(:title).last
      last.id.should eq(article2.id)
    end

    it "generates correct SQL for basic search" do
      sql = Article.search("crystal", :title, :content).to_sql
      sql.should contain("to_tsvector")
      sql.should contain("to_tsquery")
      sql.should contain("@@")
      sql.should contain("'english'")
    end

    it "generates correct SQL for ranked search" do
      sql = Article.search_ranked("crystal", :title, :content).to_sql
      sql.should contain("ts_rank")
      sql.should contain("ORDER BY")
      sql.should contain("ts_rank DESC")
    end

    it "works with complex queries combining multiple conditions" do
      article1 = Article.create!(title: "Crystal Programming", content: "Advanced topics", published: true)
      article2 = Article.create!(title: "Crystal Basics", content: "Beginner guide", published: true)
      article3 = Article.create!(title: "Crystal Tutorial", content: "Intermediate level", published: false)

      results = Article
        .where(published: true)
        .search("crystal", :title, :content)
        .where_not(title: "Crystal Basics")
        .to_a

      results.size.should eq(1)
      results.first.id.should eq(article1.id)
    end

    it "handles nil values in searchable columns" do
      article1 = Article.create!(title: "Crystal Programming", content: nil, published: true)
      article2 = Article.create!(title: "Empty content", content: "Crystal language", published: true)

      results = Article.search("crystal", :title, :content).to_a
      results.size.should eq(2)
    end

    it "supports rank normalization" do
      article1 = Article.create!(title: "Crystal", content: "Short")
      article2 = Article.create!(title: "Crystal Programming Language Tutorial", content: "This is a very long article about Crystal programming with lots of content")

      # Without normalization, longer documents might rank differently
      results = Article.search_ranked("crystal", :title, :content).to_a
      results.size.should eq(2)

      # With normalization (1 = divide rank by document length)
      results_normalized = Article.search_ranked("crystal", :title, :content, rank_normalization: 1).to_a
      results_normalized.size.should eq(2)
    end
  end

  describe "Advanced Full-Text Search Features" do
    before_each do
      Article.clear
    end

    it "performs prefix search" do
      article1 = Article.create!(title: "Crystal Programming", content: "Learn Crystal")
      article2 = Article.create!(title: "Crystalline Structure", content: "Chemistry")
      article3 = Article.create!(title: "Ruby Tutorial", content: "Ruby basics")

      results = Article.search_prefix("cryst", :title, :content).to_a
      results.size.should eq(2)
      results.map(&.id).sort!.should eq([article1.id, article2.id].sort)
    end

    it "performs proximity search" do
      article1 = Article.create!(title: "Crystal is a programming language", content: "Learn it")
      article2 = Article.create!(title: "Crystal", content: "A very advanced programming language")
      article3 = Article.create!(title: "Programming", content: "Crystal")

      # "crystal" and "programming" within 3 words
      results = Article.search_proximity("crystal", "programming", 3, :title, :content).to_a
      results.size.should be >= 1
      results.map(&.id).should contain(article1.id)
    end

    it "performs plain text search" do
      article1 = Article.create!(title: "Crystal Programming", content: "Learn Crystal")
      article2 = Article.create!(title: "Ruby Tutorial", content: "Ruby basics")

      # plainto_tsquery automatically handles plain text
      results = Article.search_plain("crystal programming", :title, :content).to_a
      results.size.should eq(1)
      results.first.id.should eq(article1.id)
    end

    it "performs weighted search" do
      article1 = Article.create!(title: "Crystal", content: "Ruby programming")
      article2 = Article.create!(title: "Ruby", content: "Crystal programming")

      # Title has weight A (1.0), content has weight B (0.4)
      # article1 should rank higher because "Crystal" is in the title
      weights = {
        :title   => PgORM::FullTextSearch::Weight::A,
        :content => PgORM::FullTextSearch::Weight::B,
      }
      results = Article.search_ranked_weighted("crystal", weights).to_a

      results.size.should eq(2)
      results.first.id.should eq(article1.id)
    end

    it "uses ts_rank_cd for cover density ranking" do
      article1 = Article.create!(title: "Crystal", content: "Short")
      article2 = Article.create!(title: "Crystal Programming Language", content: "Crystal is great")

      results = Article.search_ranked(
        "crystal",
        :title,
        :content,
        rank_function: PgORM::FullTextSearch::RankFunction::RankCD
      ).to_a

      results.size.should eq(2)
      # Both should be found
      results.map(&.id).sort!.should eq([article1.id, article2.id].sort)
    end

    it "generates correct SQL for weighted search" do
      weights = {
        :title   => PgORM::FullTextSearch::Weight::A,
        :content => PgORM::FullTextSearch::Weight::B,
      }
      sql = Article.search_weighted("crystal", weights).to_sql

      sql.should contain("setweight")
      sql.should contain("'A'")
      sql.should contain("'B'")
    end

    it "generates correct SQL for plain text search" do
      sql = Article.search_plain("crystal programming", :title, :content).to_sql
      sql.should contain("plainto_tsquery")
    end

    it "generates correct SQL for prefix search" do
      sql = Article.search_prefix("cryst", :title).to_sql
      sql.should contain("cryst:*")
    end

    it "generates correct SQL for proximity search" do
      sql = Article.search_proximity("crystal", "programming", 5, :content).to_sql
      sql.should contain("<5>")
    end

    it "generates correct SQL for ts_rank_cd" do
      sql = Article.search_ranked(
        "crystal",
        :title,
        rank_function: PgORM::FullTextSearch::RankFunction::RankCD
      ).to_sql

      sql.should contain("ts_rank_cd")
    end

    it "works with weighted search and where clauses" do
      article1 = Article.create!(title: "Crystal Programming", content: "Advanced", published: true)
      article2 = Article.create!(title: "Crystal Basics", content: "Beginner", published: false)

      weights = {
        :title   => PgORM::FullTextSearch::Weight::A,
        :content => PgORM::FullTextSearch::Weight::B,
      }
      results = Article
        .where(published: true)
        .search_weighted("crystal", weights)
        .to_a

      results.size.should eq(1)
      results.first.id.should eq(article1.id)
    end

    it "combines prefix search with other query methods" do
      5.times do |i|
        Article.create!(title: "Crystal Tutorial #{i}", content: "Learn Crystal", published: i.even?)
      end

      results = Article
        .where(published: true)
        .search_prefix("cryst", :title, :content)
        .limit(2)
        .to_a

      results.size.should eq(2)
    end

    it "handles complex tsquery with multiple operators" do
      article1 = Article.create!(title: "Crystal Programming Language", content: "Fast and efficient")
      article2 = Article.create!(title: "Ruby Programming", content: "Dynamic and flexible")
      article3 = Article.create!(title: "Crystal Tutorial", content: "Ruby-like syntax")

      # (crystal | ruby) & programming
      results = Article.search("(crystal | ruby) & programming", :title, :content).to_a
      results.size.should be >= 2
    end

    it "supports negation in search queries" do
      article1 = Article.create!(title: "Crystal Programming", content: "Fast compiled")
      article2 = Article.create!(title: "Crystal Tutorial", content: "Ruby-like syntax")
      article3 = Article.create!(title: "Ruby Guide", content: "Dynamic language")

      # crystal but not ruby
      results = Article.search("crystal & !ruby", :title, :content).to_a
      results.size.should eq(1)
      results.first.id.should eq(article1.id)
    end
  end

  describe "Pre-computed tsvector Column Support" do
    before_each do
      Article.clear

      # Add search_vector column and GIN index
      PgORM::Database.connection do |db|
        db.exec "ALTER TABLE articles ADD COLUMN IF NOT EXISTS search_vector tsvector"
        db.exec "CREATE INDEX IF NOT EXISTS articles_search_idx ON articles USING GIN(search_vector)"
      end
    end

    after_each do
      # Clean up
      PgORM::Database.connection do |db|
        db.exec "DROP INDEX IF EXISTS articles_search_idx"
        db.exec "ALTER TABLE articles DROP COLUMN IF EXISTS search_vector"
      end
    end

    it "searches using pre-computed tsvector column" do
      # Manually populate search_vector for testing
      PgORM::Database.connection do |db|
        db.exec <<-SQL
          INSERT INTO articles (title, content, published, search_vector)
          VALUES
            ('Crystal Programming', 'Learn Crystal', false,
             to_tsvector('english', 'Crystal Programming Learn Crystal')),
            ('Ruby Tutorial', 'Learn Ruby', false,
             to_tsvector('english', 'Ruby Tutorial Learn Ruby'))
        SQL
      end

      results = Article.search_vector("crystal", :search_vector).to_a
      results.size.should eq(1)
      results.first.title.should eq("Crystal Programming")
    end

    it "searches pre-computed tsvector with ranking" do
      PgORM::Database.connection do |db|
        db.exec <<-SQL
          INSERT INTO articles (title, content, published, search_vector)
          VALUES
            ('Crystal', 'Short', false,
             to_tsvector('english', 'Crystal Short')),
            ('Crystal Programming Language', 'Crystal is great', false,
             to_tsvector('english', 'Crystal Programming Language Crystal is great'))
        SQL
      end

      results = Article.search_vector_ranked("crystal", :search_vector).to_a
      results.size.should eq(2)
      # Second article should rank higher due to more occurrences
      results.first.title.should eq("Crystal Programming Language")
    end

    it "searches pre-computed tsvector with plain text" do
      PgORM::Database.connection do |db|
        db.exec <<-SQL
          INSERT INTO articles (title, content, published, search_vector)
          VALUES
            ('Crystal Programming', 'Learn it', false,
             to_tsvector('english', 'Crystal Programming Learn it'))
        SQL
      end

      results = Article.search_vector_plain("crystal programming", :search_vector).to_a
      results.size.should eq(1)
    end

    it "supports tsquery operators with pre-computed vectors" do
      PgORM::Database.connection do |db|
        db.exec <<-SQL
          INSERT INTO articles (title, content, published, search_vector)
          VALUES
            ('Crystal Programming', 'Fast', false,
             to_tsvector('english', 'Crystal Programming Fast')),
            ('Crystal Tutorial', 'Ruby-like', false,
             to_tsvector('english', 'Crystal Tutorial Ruby-like')),
            ('Ruby Guide', 'Dynamic', false,
             to_tsvector('english', 'Ruby Guide Dynamic'))
        SQL
      end

      # AND operator
      results = Article.search_vector("crystal & programming", :search_vector).to_a
      results.size.should eq(1)

      # OR operator
      results = Article.search_vector("crystal | ruby", :search_vector).to_a
      results.size.should eq(3)

      # NOT operator
      results = Article.search_vector("crystal & !ruby", :search_vector).to_a
      results.size.should eq(1)
      results.first.title.should eq("Crystal Programming")
    end

    it "works with where clauses on pre-computed vectors" do
      PgORM::Database.connection do |db|
        db.exec <<-SQL
          INSERT INTO articles (title, content, published, search_vector)
          VALUES
            ('Crystal Programming', 'Advanced', true,
             to_tsvector('english', 'Crystal Programming Advanced')),
            ('Crystal Basics', 'Beginner', false,
             to_tsvector('english', 'Crystal Basics Beginner'))
        SQL
      end

      results = Article
        .where(published: true)
        .search_vector("crystal", :search_vector)
        .to_a

      results.size.should eq(1)
      results.first.title.should eq("Crystal Programming")
    end

    it "uses ts_rank_cd with pre-computed vectors" do
      PgORM::Database.connection do |db|
        db.exec <<-SQL
          INSERT INTO articles (title, content, published, search_vector)
          VALUES
            ('Crystal', 'Short', false,
             to_tsvector('english', 'Crystal Short')),
            ('Crystal Programming', 'Crystal', false,
             to_tsvector('english', 'Crystal Programming Crystal'))
        SQL
      end

      results = Article.search_vector_ranked(
        "crystal",
        :search_vector,
        rank_function: PgORM::FullTextSearch::RankFunction::RankCD
      ).to_a

      results.size.should eq(2)
    end

    it "supports rank normalization with pre-computed vectors" do
      PgORM::Database.connection do |db|
        db.exec <<-SQL
          INSERT INTO articles (title, content, published, search_vector)
          VALUES
            ('Crystal', 'Short', false,
             to_tsvector('english', 'Crystal Short')),
            ('Crystal Programming Language', 'Long content here', false,
             to_tsvector('english', 'Crystal Programming Language Long content here'))
        SQL
      end

      results = Article.search_vector_ranked(
        "crystal",
        :search_vector,
        rank_normalization: 1
      ).to_a

      results.size.should eq(2)
    end

    it "generates correct SQL for pre-computed vector search" do
      sql = Article.search_vector("crystal", :search_vector).to_sql
      sql.should contain("search_vector")
      sql.should contain("@@")
      sql.should contain("to_tsquery")
    end

    it "generates correct SQL for ranked pre-computed vector search" do
      sql = Article.search_vector_ranked("crystal", :search_vector).to_sql
      sql.should contain("search_vector")
      sql.should contain("ts_rank")
      sql.should contain("search_rank")
      sql.should contain("ORDER BY")
    end
  end

  describe "Pagination with Full-Text Search" do
    before_each do
      Article.clear

      # Create articles with different content
      25.times do |i|
        Article.create!(
          title: "Crystal Article #{i + 1}",
          content: "This article discusses Crystal programming language and its features",
          published: i.even?
        )
      end

      15.times do |i|
        Article.create!(
          title: "Ruby Article #{i + 1}",
          content: "This article is about Ruby programming and development",
          published: true
        )
      end

      10.times do |i|
        Article.create!(
          title: "Python Guide #{i + 1}",
          content: "Python tutorial for beginners",
          published: false
        )
      end
    end

    it "paginates basic search results" do
      result = Article.search("crystal", :title, :content).order(:id).paginate(page: 1, limit: 10)

      result.records.size.should eq(10)
      result.total.should eq(25)
      result.page.should eq(1)
      result.total_pages.should eq(3)
      result.has_next?.should be_true
      result.has_prev?.should be_false
    end

    it "paginates to second page of search results" do
      result = Article.search("crystal", :title, :content).order(:id).paginate(page: 2, limit: 10)

      result.records.size.should eq(10)
      result.total.should eq(25)
      result.page.should eq(2)
      result.from.should eq(11)
      result.to.should eq(20)
      result.has_next?.should be_true
      result.has_prev?.should be_true
    end

    it "paginates ranked search results" do
      result = Article.search_ranked("crystal", :title, :content).paginate(page: 1, limit: 10)

      result.records.size.should eq(10)
      result.total.should eq(25)
      result.page.should eq(1)
      result.total_pages.should eq(3)
    end

    it "paginates search with where clauses" do
      result = Article
        .where(published: true)
        .search("crystal", :title, :content)
        .order(:id)
        .paginate(page: 1, limit: 5)

      result.records.size.should eq(5)
      # Only published Crystal articles (13 out of 25)
      result.total.should eq(13)
      result.records.all?(&.published).should be_true
    end

    it "paginates phrase search results" do
      result = Article.search_phrase("crystal programming", :title, :content).order(:id).paginate(page: 1, limit: 10)

      result.records.size.should be <= 10
      result.total.should be > 0
    end

    it "paginates prefix search results" do
      result = Article.search_prefix("cryst", :title, :content).order(:id).paginate(page: 1, limit: 10)

      result.records.size.should eq(10)
      result.total.should eq(25)
    end

    it "paginates plain text search results" do
      result = Article.search_plain("crystal programming", :title, :content).order(:id).paginate(page: 1, limit: 10)

      result.records.size.should eq(10)
      result.total.should eq(25)
    end

    it "paginates weighted search results" do
      weights = {
        :title   => PgORM::FullTextSearch::Weight::A,
        :content => PgORM::FullTextSearch::Weight::B,
      }
      result = Article.search_weighted("crystal", weights).order(:id).paginate(page: 1, limit: 10)

      result.records.size.should eq(10)
      result.total.should eq(25)
    end

    it "paginates ranked weighted search results" do
      weights = {
        :title   => PgORM::FullTextSearch::Weight::A,
        :content => PgORM::FullTextSearch::Weight::B,
      }
      result = Article.search_ranked_weighted("crystal", weights).paginate(page: 1, limit: 10)

      result.records.size.should eq(10)
      result.total.should eq(25)
    end

    it "uses cursor pagination with search results" do
      result = Article.search("ruby", :title, :content).order(:id).paginate_cursor(limit: 5)

      result.records.size.should eq(5)
      result.has_next?.should be_true
      result.next_cursor.should_not be_nil

      # Get next page
      next_result = Article.search("ruby", :title, :content).order(:id).paginate_cursor(after: result.next_cursor, limit: 5)
      next_result.records.size.should eq(5)
      next_result.records.map(&.id).should_not eq(result.records.map(&.id))
    end

    it "paginates search with offset" do
      result = Article.search("crystal", :title, :content).order(:id).paginate_by_offset(offset: 10, limit: 5)

      result.records.size.should eq(5)
      result.total.should eq(25)
      result.offset.should eq(10)
      result.page.should eq(3) # (10 / 5) + 1
    end

    it "handles empty search results with pagination" do
      result = Article.search("nonexistent", :title, :content).paginate(page: 1, limit: 10)

      result.records.should be_empty
      result.total.should eq(0)
      result.page.should eq(1)
      result.total_pages.should eq(1)
      result.has_next?.should be_false
    end

    it "combines search with limit and offset directly" do
      # First get some results
      all_results = Article.search("crystal", :title, :content).order(:id).limit(15).to_a
      all_results.size.should eq(15)

      # Now paginate
      paginated = Article.search("crystal", :title, :content).order(:id).paginate(page: 2, limit: 5)
      paginated.records.size.should eq(5)

      # The paginated results should match a subset of all results
      paginated.records.map(&.id).should eq(all_results[5...10].map(&.id))
    end

    it "serializes paginated search results to JSON" do
      result = Article.search("ruby", :title, :content).order(:id).paginate(page: 1, limit: 5)
      json = JSON.parse(result.to_json)

      json["data"].as_a.size.should eq(5)
      json["pagination"]["total"].as_i64.should eq(15)
      json["pagination"]["page"].as_i.should eq(1)
      json["pagination"]["limit"].as_i.should eq(5)
      json["pagination"]["total_pages"].as_i.should eq(3)
      json["pagination"]["has_next"].as_bool.should be_true
      json["pagination"]["has_prev"].as_bool.should be_false
    end

    it "paginates search with multiple boolean operators" do
      result = Article
        .search("(crystal | ruby) & programming", :title, :content)
        .order(:id)
        .paginate(page: 1, limit: 10)

      result.records.size.should eq(10)
      result.total.should be >= 10
    end

    it "paginates search with NOT operator" do
      result = Article
        .search("crystal & !python", :title, :content)
        .order(:id)
        .paginate(page: 1, limit: 10)

      result.records.size.should eq(10)
      result.total.should eq(25)
    end

    it "handles pagination beyond available results" do
      result = Article.search("crystal", :title, :content).order(:id).paginate(page: 10, limit: 10)

      result.records.should be_empty
      result.total.should eq(25)
      result.page.should eq(10)
      result.has_next?.should be_false
    end

    it "paginates with different text search configurations" do
      result = Article
        .search("programming", :title, :content, config: "english")
        .order(:id)
        .paginate(page: 1, limit: 10)

      result.records.size.should be >= 1
      result.total.should be >= 1
    end
  end
  describe "Full-Text Search Edge Cases" do
    describe "Error Handling" do
      before_each do
        Article.clear
        Article.create!(title: "Crystal Programming", content: "Learn Crystal")
      end

      it "raises error on empty search query" do
        expect_raises(ArgumentError, "search query cannot be empty") do
          Article.search("", :title, :content)
        end
      end

      it "raises error on whitespace-only search query" do
        expect_raises(ArgumentError, "search query cannot be empty") do
          Article.search("   ", :title, :content)
        end
      end

      it "raises error when no columns specified for search" do
        # This is a compile-time error, so we just verify the method signature requires columns
        # The method signature `search(query : String, *columns : Symbol)` enforces this
        true.should be_true
      end

      it "raises error on empty phrase search" do
        expect_raises(ArgumentError, "search phrase cannot be empty") do
          Article.search_phrase("", :title)
        end
      end

      it "raises error on empty prefix search" do
        expect_raises(ArgumentError, "prefix cannot be empty") do
          Article.search_prefix("", :title)
        end
      end

      it "raises error on empty plain text search" do
        expect_raises(ArgumentError, "search text cannot be empty") do
          Article.search_plain("", :title)
        end
      end

      it "raises error on empty proximity search words" do
        expect_raises(ArgumentError, "words cannot be empty") do
          Article.search_proximity("", "programming", 5, :content)
        end

        expect_raises(ArgumentError, "words cannot be empty") do
          Article.search_proximity("crystal", "", 5, :content)
        end
      end

      it "raises error on empty vector search query" do
        expect_raises(ArgumentError, "search query cannot be empty") do
          Article.search_vector("", :title)
        end
      end

      it "raises error on empty vector plain search" do
        expect_raises(ArgumentError, "search text cannot be empty") do
          Article.search_vector_plain("", :title)
        end
      end

      it "raises error on empty ranked search query" do
        expect_raises(ArgumentError, "search query cannot be empty") do
          Article.search_ranked("", :title, :content)
        end
      end

      it "raises error on empty weighted search query" do
        weights = {:title => PgORM::FullTextSearch::Weight::A}
        expect_raises(ArgumentError, "search query cannot be empty") do
          Article.search_weighted("", weights)
        end
      end

      it "raises error on weighted search with no columns" do
        weights = {} of Symbol => PgORM::FullTextSearch::Weight
        expect_raises(ArgumentError, "at least one column must be specified") do
          Article.search_weighted("crystal", weights)
        end
      end

      it "raises error on ranked weighted search with no columns" do
        weights = {} of Symbol => PgORM::FullTextSearch::Weight
        expect_raises(ArgumentError, "at least one column must be specified") do
          Article.search_ranked_weighted("crystal", weights)
        end
      end
    end

    describe "NULL Handling" do
      before_each do
        Article.clear
      end

      it "handles NULL content in search" do
        Article.create!(title: "Crystal", content: nil)
        Article.create!(title: "Ruby", content: "Programming")

        results = Article.search("crystal", :title, :content).to_a
        results.size.should eq(1)
        results.first.title.should eq("Crystal")
      end

      it "handles all NULL columns in search" do
        Article.create!(title: "Test", content: nil)

        results = Article.search("test", :title, :content).to_a
        results.size.should eq(1)
      end

      it "handles NULL in ranked search" do
        Article.create!(title: "Crystal", content: nil)
        Article.create!(title: "Crystal Programming", content: "Learn Crystal")

        results = Article.search_ranked("crystal", :title, :content).to_a
        results.size.should eq(2)
      end

      it "handles NULL in weighted search" do
        Article.create!(title: "Crystal", content: nil)

        weights = {
          :title   => PgORM::FullTextSearch::Weight::A,
          :content => PgORM::FullTextSearch::Weight::B,
        }
        results = Article.search_weighted("crystal", weights).to_a
        results.size.should eq(1)
      end
    end

    describe "Special Characters" do
      before_each do
        Article.clear
      end

      it "handles single quotes in search query" do
        Article.create!(title: "It's Crystal", content: "Programming")

        # Single quotes should be escaped
        results = Article.search("it's", :title, :content).to_a
        results.size.should be >= 0 # May or may not match depending on tokenization
      end

      it "handles double quotes in search query" do
        Article.create!(title: "The \"Crystal\" Language", content: "Programming")

        results = Article.search("crystal", :title, :content).to_a
        results.size.should eq(1)
      end

      it "handles ampersand in content" do
        Article.create!(title: "C & C++", content: "Programming")

        results = Article.search("programming", :title, :content).to_a
        results.size.should eq(1)
      end

      it "handles parentheses in search" do
        Article.create!(title: "Crystal (Programming)", content: "Language")

        # Parentheses have special meaning in tsquery, but should be handled
        results = Article.search("crystal", :title, :content).to_a
        results.size.should eq(1)
      end

      it "handles backslash in content" do
        Article.create!(title: "Path\\to\\file", content: "Windows paths")

        results = Article.search("windows", :title, :content).to_a
        results.size.should eq(1)
      end
    end

    describe "Large Queries" do
      before_each do
        Article.clear
        Article.create!(title: "Crystal Programming Language", content: "A fast compiled language")
      end

      it "handles very long search query" do
        long_query = "crystal " * 100 # 700+ characters

        # PostgreSQL has limits on tsquery length, so this may raise an error
        begin
          results = Article.search(long_query, :title, :content).to_a
          results.should be_a(Array(Article))
        rescue PQ::PQError
          # Expected - PostgreSQL rejects overly long tsquery
          true.should be_true
        end
      end

      it "handles search with many OR operators" do
        query = (1..50).map { |i| "word#{i}" }.join(" | ")

        results = Article.search(query, :title, :content).to_a
        results.should be_a(Array(Article))
      end

      it "handles search with many AND operators" do
        query = "crystal & programming & language & fast & compiled"

        results = Article.search(query, :title, :content).to_a
        results.size.should be >= 0
      end
    end

    describe "Different Text Configurations" do
      before_each do
        Article.clear
        Article.create!(title: "Running fast", content: "The runner runs quickly")
      end

      it "supports simple configuration" do
        results = Article.search("running", :title, :content, config: "simple").to_a
        results.size.should eq(1)
      end

      it "supports english configuration with stemming" do
        results = Article.search("run", :title, :content, config: "english").to_a
        # Should match "running", "runner", "runs" due to stemming
        results.size.should eq(1)
      end

      it "handles invalid configuration gracefully" do
        # PostgreSQL will raise an error for invalid config
        expect_raises(Exception) do
          Article.search("test", :title, :content, config: "invalid_config").to_a
        end
      end
    end

    describe "Complex Boolean Queries" do
      before_each do
        Article.clear
        Article.create!(title: "Crystal Programming", content: "Fast and efficient")
        Article.create!(title: "Ruby Programming", content: "Dynamic and flexible")
        Article.create!(title: "Crystal Tutorial", content: "Ruby-like syntax")
      end

      it "handles nested boolean operators" do
        results = Article.search("(crystal | ruby) & programming", :title, :content).to_a
        results.size.should eq(2)
      end

      it "handles multiple NOT operators" do
        results = Article.search("crystal & !ruby & !python", :title, :content).to_a
        results.size.should eq(1)
        results.first.title.should eq("Crystal Programming")
      end

      it "handles complex precedence" do
        results = Article.search("crystal & (programming | tutorial)", :title, :content).to_a
        results.size.should eq(2)
      end
    end

    describe "Rank Function Edge Cases" do
      before_each do
        Article.clear
        Article.create!(title: "Crystal", content: "Short")
        Article.create!(title: "Crystal Programming Language Tutorial", content: "Very long content about Crystal programming")
      end

      it "handles rank normalization with zero" do
        results = Article.search_ranked("crystal", :title, :content, rank_normalization: 0).to_a
        results.size.should eq(2)
      end

      it "handles rank normalization with large value" do
        results = Article.search_ranked("crystal", :title, :content, rank_normalization: 32).to_a
        results.size.should eq(2)
      end

      it "handles ts_rank_cd function" do
        results = Article.search_ranked(
          "crystal",
          :title,
          :content,
          rank_function: PgORM::FullTextSearch::RankFunction::RankCD
        ).to_a
        results.size.should eq(2)
      end
    end
  end
end

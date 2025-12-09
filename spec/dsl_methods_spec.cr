require "./spec_helper"

module PgORM
  describe "DSL Methods" do
    before_each do
      Author.truncate
    end

    describe "OR queries" do
      it "combines two where conditions with OR" do
        author1 = Author.create!(name: "John")
        author2 = Author.create!(name: "Jane")
        author3 = Author.create!(name: "Bob")

        results = Author.where(name: "John").or(Author.where(name: "Jane")).to_a
        results.size.should eq(2)
        results.map(&.name).sort!.should eq(["Jane", "John"])
      end

      it "generates correct SQL with OR" do
        sql = Author.where(name: "John").or(Author.where(name: "Jane")).to_sql
        sql.should contain("OR")
        sql.should contain("name")
      end
    end

    describe "LIKE pattern matching" do
      it "matches patterns with LIKE (case-sensitive)" do
        author1 = Author.create!(name: "John Doe")
        author2 = Author.create!(name: "Jane Doe")
        author3 = Author.create!(name: "Bob Smith")

        results = Author.where_like(:name, "%Doe").to_a
        results.size.should eq(2)
        results.map(&.name).sort!.should eq(["Jane Doe", "John Doe"])
      end

      it "matches prefix patterns" do
        author1 = Author.create!(name: "Alice")
        author2 = Author.create!(name: "Alicia")
        author3 = Author.create!(name: "Bob")

        results = Author.where_like(:name, "Ali%").to_a
        results.size.should eq(2)
      end
    end

    describe "ILIKE pattern matching" do
      it "matches patterns case-insensitively" do
        author1 = Author.create!(name: "John DOE")
        author2 = Author.create!(name: "jane doe")
        author3 = Author.create!(name: "Bob Smith")

        results = Author.where_ilike(:name, "%doe").to_a
        results.size.should eq(2)
      end

      it "works with domain-like strings" do
        author1 = Author.create!(name: "api.EXAMPLE.com")
        author2 = Author.create!(name: "web.example.COM")
        author3 = Author.create!(name: "test.other.com")

        results = Author.where_ilike(:name, "%example%").to_a
        results.size.should eq(2)
      end
    end

    describe "Comparison operators" do
      it "filters with greater than" do
        author1 = Author.create!(name: "Author1")
        author2 = Author.create!(name: "Author2")
        author3 = Author.create!(name: "Author3")

        results = Author.where_gt(:id, author1.id).to_a
        results.size.should eq(2)
      end

      it "filters with BETWEEN" do
        author1 = Author.create!(name: "Author1")
        author2 = Author.create!(name: "Author2")
        author3 = Author.create!(name: "Author3")
        author4 = Author.create!(name: "Author4")

        results = Author.where_between(:id, author2.id, author3.id).to_a
        results.size.should eq(2)
      end
    end

    describe "Combining DSL methods" do
      it "combines OR with LIKE" do
        author1 = Author.create!(name: "John Smith")
        author2 = Author.create!(name: "Jane Doe")
        author3 = Author.create!(name: "Bob Johnson")

        results = Author.where_like(:name, "%Smith")
          .or(Author.where_like(:name, "%Doe"))
          .to_a

        results.size.should eq(2)
        results.map(&.name).sort!.should eq(["Jane Doe", "John Smith"])
      end
    end

    describe "Real-world use cases" do
      it "searches domains with partial matching" do
        author1 = Author.create!(name: "api.example.com")
        author2 = Author.create!(name: "web.example.org")
        author3 = Author.create!(name: "test.other.com")
        author4 = Author.create!(name: "example.net")

        # Find all domains containing "example"
        results = Author.where_ilike(:name, "%example%").to_a
        results.size.should eq(3)
      end

      it "searches across multiple fields with OR (DSL equivalent of raw SQL)" do
        Article.truncate

        article1 = Article.create!(title: "Crystal Language", content: "A fast compiled language", published: true)
        article2 = Article.create!(title: "Ruby Tutorial", content: "Learn Crystal programming", published: true)
        article3 = Article.create!(title: "Python Guide", content: "Python basics", published: true)

        # DSL approach: Search for "crystal" in title OR content
        search_term = "crystal"
        # Escape special SQL wildcards
        escaped = search_term.gsub("\\", "\\\\").gsub("%", "\\%").gsub("_", "\\_")
        pattern = "%#{escaped}%"

        # Using DSL with OR - much cleaner than raw SQL!
        results = Article.where_ilike(:title, pattern)
          .or(Article.where_ilike(:content, pattern))
          .to_a

        results.size.should eq(2)
        results.map(&.title).sort!.should eq(["Crystal Language", "Ruby Tutorial"])
      end

      it "searches multiple fields with special characters properly escaped" do
        # Test that special SQL wildcards are handled correctly
        author1 = Author.create!(name: "user@example.com")
        author2 = Author.create!(name: "admin_test")
        author3 = Author.create!(name: "test%user")
        author4 = Author.create!(name: "test_user")

        # Search for literal "test%" (not "test" followed by anything)
        search_term = "test%"
        escaped = search_term.gsub("\\", "\\\\").gsub("%", "\\%").gsub("_", "\\_")
        pattern = "%#{escaped}%"

        results = Author.where_ilike(:name, pattern).to_a
        results.size.should eq(1)
        results.first.name.should eq("test%user")

        # Search for literal "test_" (not "test" followed by any single char)
        search_term2 = "test_"
        escaped2 = search_term2.gsub("\\", "\\\\").gsub("%", "\\%").gsub("_", "\\_")
        pattern2 = "%#{escaped2}%"

        results2 = Author.where_ilike(:name, pattern2).to_a
        results2.size.should eq(1)
        results2.first.name.should eq("test_user")
      end

      it "chains OR across multiple fields with additional filters" do
        Article.truncate
        # Complex query: search in multiple fields AND apply other conditions
        article1 = Article.create!(title: "Crystal Programming", content: "Fast language", published: true)
        article2 = Article.create!(title: "Web Development", content: "Crystal framework guide", published: true)
        article3 = Article.create!(title: "Crystal Basics", content: "Introduction", published: false)

        search_term = "crystal"
        pattern = "%#{search_term}%"

        # Correct approach: Apply filter first, then OR search
        # This generates: WHERE published = true AND (title ILIKE '%crystal%' OR content ILIKE '%crystal%')
        results = Article.where(published: true)
          .where_ilike(:title, pattern)
          .or(Article.where(published: true).where_ilike(:content, pattern))
          .to_a

        results.size.should eq(2)
        results.all?(&.published).should be_true
        results.map(&.title).sort!.should eq(["Crystal Programming", "Web Development"])
      end
    end
  end
end

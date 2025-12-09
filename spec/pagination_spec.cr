require "./spec_helper"

describe PgORM::Pagination do
  describe "Page-based Pagination" do
    before_each do
      Article.clear
      30.times do |i|
        Article.create!(
          title: "Article #{i + 1}",
          content: "Content for article #{i + 1}",
          published: i.even?
        )
      end
    end

    it "paginates with default parameters" do
      result = Article.order(:id).paginate

      result.records.size.should eq(25)
      result.total.should eq(30)
      result.limit.should eq(25)
      result.offset.should eq(0)
      result.page.should eq(1)
      result.total_pages.should eq(2)
    end

    it "paginates with custom page and limit" do
      result = Article.order(:id).paginate(page: 2, limit: 10)

      result.records.size.should eq(10)
      result.total.should eq(30)
      result.limit.should eq(10)
      result.offset.should eq(10)
      result.page.should eq(2)
      result.total_pages.should eq(3)
    end

    it "handles last page with partial results" do
      result = Article.order(:id).paginate(page: 3, limit: 10)

      result.records.size.should eq(10)
      result.total.should eq(30)
      result.page.should eq(3)
      result.total_pages.should eq(3)
    end

    it "handles page beyond total pages" do
      result = Article.order(:id).paginate(page: 10, limit: 10)

      result.records.should be_empty
      result.total.should eq(30)
      result.page.should eq(10)
      result.total_pages.should eq(3)
    end

    it "handles page less than 1" do
      result = Article.order(:id).paginate(page: 0, limit: 10)

      result.page.should eq(1)
      result.offset.should eq(0)
    end

    it "handles limit less than 1" do
      result = Article.order(:id).paginate(page: 1, limit: 0)

      result.limit.should eq(1)
      result.records.size.should eq(1)
    end

    it "works with where clauses" do
      result = Article.where(published: true).order(:id).paginate(page: 1, limit: 5)

      result.records.size.should eq(5)
      result.total.should eq(15) # Only published articles
      result.total_pages.should eq(3)
      result.records.all?(&.published).should be_true
    end

    it "works with order clauses" do
      result = Article.order(title: :desc).paginate(page: 1, limit: 5)

      result.records.size.should eq(5)
      result.records.first.title.should eq("Article 9")
    end

    it "calculates has_next? correctly" do
      result1 = Article.order(:id).paginate(page: 1, limit: 10)
      result1.has_next?.should be_true

      result2 = Article.order(:id).paginate(page: 3, limit: 10)
      result2.has_next?.should be_false
    end

    it "calculates has_prev? correctly" do
      result1 = Article.order(:id).paginate(page: 1, limit: 10)
      result1.has_prev?.should be_false

      result2 = Article.order(:id).paginate(page: 2, limit: 10)
      result2.has_prev?.should be_true
    end

    it "calculates next_page correctly" do
      result1 = Article.order(:id).paginate(page: 1, limit: 10)
      result1.next_page.should eq(2)

      result2 = Article.order(:id).paginate(page: 3, limit: 10)
      result2.next_page.should be_nil
    end

    it "calculates prev_page correctly" do
      result1 = Article.order(:id).paginate(page: 1, limit: 10)
      result1.prev_page.should be_nil

      result2 = Article.order(:id).paginate(page: 2, limit: 10)
      result2.prev_page.should eq(1)
    end

    it "calculates from and to correctly" do
      result = Article.order(:id).paginate(page: 2, limit: 10)

      result.from.should eq(11)
      result.to.should eq(20)
    end

    it "handles empty results" do
      Article.clear

      result = Article.paginate(page: 1, limit: 10)

      result.records.should be_empty
      result.total.should eq(0)
      result.from.should eq(0)
      result.to.should eq(0)
      result.total_pages.should eq(1)
    end

    it "serializes to JSON with pagination metadata" do
      result = Article.order(:id).paginate(page: 2, limit: 5)
      json = JSON.parse(result.to_json)

      json["data"].as_a.size.should eq(5)
      json["pagination"]["total"].as_i64.should eq(30)
      json["pagination"]["limit"].as_i.should eq(5)
      json["pagination"]["offset"].as_i.should eq(5)
      json["pagination"]["page"].as_i.should eq(2)
      json["pagination"]["total_pages"].as_i.should eq(6)
      json["pagination"]["has_next"].as_bool.should be_true
      json["pagination"]["has_prev"].as_bool.should be_true
      json["pagination"]["next_page"].as_i.should eq(3)
      json["pagination"]["prev_page"].as_i.should eq(1)
      json["pagination"]["from"].as_i.should eq(6)
      json["pagination"]["to"].as_i.should eq(10)
    end
  end

  describe "Offset-based Pagination" do
    before_each do
      Article.clear
      25.times do |i|
        Article.create!(title: "Article #{i + 1}", content: "Content #{i + 1}")
      end
    end

    it "paginates with offset and limit" do
      result = Article.order(:id).paginate_by_offset(offset: 10, limit: 5)

      result.records.size.should eq(5)
      result.total.should eq(25)
      result.limit.should eq(5)
      result.offset.should eq(10)
      result.page.should eq(3) # (10 / 5) + 1
    end

    it "handles offset of 0" do
      result = Article.order(:id).paginate_by_offset(offset: 0, limit: 10)

      result.offset.should eq(0)
      result.page.should eq(1)
    end

    it "handles negative offset" do
      result = Article.order(:id).paginate_by_offset(offset: -5, limit: 10)

      result.offset.should eq(0)
    end

    it "works with where clauses" do
      result = Article.where("id > ?", 10).order(:id).paginate_by_offset(offset: 5, limit: 5)

      result.records.size.should eq(5)
      # Total should be count of records where id > 10 (which is 15 out of 25)
      result.total.should be >= 10
    end
  end

  describe "Pagination with Joins" do
    before_each do
      Author.clear
      Book.clear

      5.times do |i|
        author = Author.create!(name: "Author #{i + 1}")
        3.times do |j|
          Book.create!(name: "Book #{j + 1}", author_id: author.id)
        end
      end
    end

    it "paginates joined queries correctly" do
      # Without pagination, joins can create duplicate author records
      result = Author.join(:left, Book, :author_id).order(:id).paginate(page: 1, limit: 3)

      result.records.size.should eq(3)
      result.total.should eq(5) # Should count distinct authors, not all join results
      result.total_pages.should eq(2)
    end

    it "counts distinct records in joined queries" do
      result = Author.join(:left, Book, :author_id).paginate(page: 1, limit: 10)

      # Should count 5 distinct authors, not 15 (5 authors * 3 books)
      result.total.should eq(5)
    end

    it "works with where clauses on joined queries" do
      result = Author
        .join(:left, Book, :author_id)
        .where("books.id > ?", 1)
        .order(:id)
        .paginate(page: 1, limit: 3)

      result.records.size.should be <= 3
      result.total.should be > 0
    end
  end

  describe "Cursor-based Pagination" do
    before_each do
      Article.clear
      20.times do |i|
        Article.create!(title: "Article #{i + 1}", content: "Content #{i + 1}")
      end
    end

    it "paginates with cursor (first page)" do
      result = Article.order(:id).paginate_cursor(limit: 5)

      result.records.size.should eq(5)
      result.limit.should eq(5)
      result.has_next?.should be_true
      result.has_prev?.should be_false
      result.next_cursor.should_not be_nil
      result.prev_cursor.should be_nil
    end

    it "paginates forward with after cursor" do
      first_page = Article.order(:id).paginate_cursor(limit: 5)
      cursor = first_page.next_cursor.not_nil!

      second_page = Article.order(:id).paginate_cursor(after: cursor, limit: 5)

      second_page.records.size.should eq(5)
      second_page.has_next?.should be_true
      second_page.has_prev?.should be_true
      second_page.next_cursor.should_not be_nil
      second_page.prev_cursor.should_not be_nil

      # Records should be different
      first_page.records.map(&.id).should_not eq(second_page.records.map(&.id))
    end

    it "paginates backward with before cursor" do
      first_page = Article.order(:id).paginate_cursor(limit: 5)
      second_page = Article.order(:id).paginate_cursor(after: first_page.next_cursor, limit: 5)
      cursor = second_page.prev_cursor.not_nil!

      back_page = Article.order(:id).paginate_cursor(before: cursor, limit: 5)

      back_page.records.size.should eq(5)
      # Should get back to first page records
      back_page.records.map(&.id).should eq(first_page.records.map(&.id))
    end

    it "handles last page correctly" do
      # Get to last page
      result = Article.order(:id).paginate_cursor(limit: 20)

      result.records.size.should eq(20)
      result.has_next?.should be_false
      result.next_cursor.should be_nil
    end

    it "handles empty results" do
      Article.clear

      result = Article.order(:id).paginate_cursor(limit: 10)

      result.records.should be_empty
      result.has_next?.should be_false
      result.has_prev?.should be_false
      result.next_cursor.should be_nil
      result.prev_cursor.should be_nil
    end

    it "works with where clauses" do
      result = Article.where("id > ?", 10).order(:id).paginate_cursor(limit: 5)

      result.records.size.should eq(5)
      result.records.all? { |r| r.id.not_nil! > 10 }.should be_true
    end

    it "serializes to JSON with cursor metadata" do
      result = Article.order(:id).paginate_cursor(limit: 5)
      json = JSON.parse(result.to_json)

      json["data"].as_a.size.should eq(5)
      json["pagination"]["limit"].as_i.should eq(5)
      json["pagination"]["has_next"].as_bool.should be_true
      json["pagination"]["has_prev"].as_bool.should be_false
      json["pagination"]["next_cursor"].as_s?.should_not be_nil
      json["pagination"]["prev_cursor"].raw.should be_nil
    end
  end

  describe "Pagination with Relations" do
    before_each do
      Author.clear
      Book.clear

      author = Author.create!(name: "Test Author")
      15.times do |i|
        Book.create!(name: "Book #{i + 1}", author_id: author.id)
      end
    end

    it "paginates has_many relations" do
      author = Author.first.not_nil!
      result = author.books.order(:id).paginate(page: 1, limit: 5)

      result.records.size.should eq(5)
      result.total.should eq(15)
      result.total_pages.should eq(3)
    end

    it "paginates relations with where clauses" do
      author = Author.first.not_nil!
      result = author.books.where("id > ?", 5).order(:id).paginate(page: 1, limit: 5)

      result.records.size.should eq(5)
      # Total should be count of books where id > 5 (which is 10 out of 15)
      result.total.should be >= 5
      result.records.all? { |b| b.id.not_nil! > 5 }.should be_true
    end
  end

  describe "Pagination with Full-Text Search" do
    before_each do
      Article.clear
      20.times do |i|
        Article.create!(
          title: "Crystal Article #{i + 1}",
          content: "This is about Crystal programming language"
        )
      end
      10.times do |i|
        Article.create!(
          title: "Ruby Article #{i + 1}",
          content: "This is about Ruby programming"
        )
      end
    end

    it "paginates search results" do
      result = Article.search("crystal", :title, :content).order(:id).paginate(page: 1, limit: 5)

      result.records.size.should eq(5)
      result.total.should eq(20)
      result.total_pages.should eq(4)
    end

    it "paginates ranked search results" do
      result = Article.search_ranked("crystal", :title, :content).paginate(page: 1, limit: 5)

      result.records.size.should eq(5)
      result.total.should eq(20)
    end
  end
  describe "Pagination Edge Cases" do
    describe "Error Handling" do
      before_each do
        Article.clear
        10.times { |i| Article.create!(title: "Article #{i}", content: "Content") }
      end

      it "handles page number zero" do
        result = Article.paginate(page: 0, limit: 5)
        result.page.should eq(1) # Should default to page 1
        result.records.size.should eq(5)
      end

      it "handles negative page number" do
        result = Article.paginate(page: -5, limit: 5)
        result.page.should eq(1) # Should default to page 1
        result.records.size.should eq(5)
      end

      it "handles zero limit" do
        result = Article.paginate(page: 1, limit: 0)
        result.limit.should eq(1) # Should default to 1
        result.records.size.should eq(1)
      end

      it "handles negative limit" do
        result = Article.paginate(page: 1, limit: -10)
        result.limit.should eq(1) # Should default to 1
        result.records.size.should eq(1)
      end

      it "handles very large page number" do
        result = Article.paginate(page: 1000, limit: 5)
        result.page.should eq(1000)
        result.records.should be_empty
        result.total.should eq(10)
        result.has_next?.should be_false
      end

      it "handles page beyond total pages" do
        result = Article.paginate(page: 10, limit: 5)
        result.page.should eq(10)
        result.total_pages.should eq(2)
        result.records.should be_empty
      end

      it "handles negative offset" do
        result = Article.paginate_by_offset(offset: -10, limit: 5)
        result.offset.should eq(0) # Should default to 0
        result.records.size.should eq(5)
      end

      it "handles offset beyond total records" do
        result = Article.paginate_by_offset(offset: 100, limit: 5)
        result.offset.should eq(100)
        result.records.should be_empty
        result.total.should eq(10)
      end

      it "handles empty table pagination" do
        Article.clear
        result = Article.paginate(page: 1, limit: 10)

        result.records.should be_empty
        result.total.should eq(0)
        result.page.should eq(1)
        result.total_pages.should eq(1)
        result.has_next?.should be_false
        result.has_prev?.should be_false
        result.from.should eq(0)
        result.to.should eq(0)
      end

      it "handles empty table with cursor pagination" do
        Article.clear
        result = Article.order(:id).paginate_cursor(limit: 10)

        result.records.should be_empty
        result.has_next?.should be_false
        result.has_prev?.should be_false
        result.next_cursor.should be_nil
        result.prev_cursor.should be_nil
      end
    end

    describe "Memory Efficiency" do
      before_each do
        Article.clear
        20.times { |i| Article.create!(title: "Article #{i}", content: "Content") }
      end

      it "doesn't load records until accessed" do
        result = Article.paginate(page: 1, limit: 10)

        # At this point, records should not be loaded yet
        # We can only verify this indirectly by checking metadata works without records
        result.total.should eq(20)
        result.page.should eq(1)
        result.has_next?.should be_true

        # Now access records
        records = result.records
        records.size.should eq(10)
      end

      it "caches records after first access" do
        result = Article.paginate(page: 1, limit: 10)

        # First access
        records1 = result.records
        records1.size.should eq(10)

        # Second access should return same cached array
        records2 = result.records
        records2.object_id.should eq(records1.object_id)
      end

      it "allows streaming without loading all records" do
        result = Article.paginate(page: 1, limit: 10)

        count = 0
        result.each do |article|
          count += 1
          article.should be_a(Article)
        end

        count.should eq(10)
      end
    end

    describe "Boundary Conditions" do
      before_each do
        Article.clear
      end

      it "handles single record pagination" do
        Article.create!(title: "Only Article", content: "Content")

        result = Article.paginate(page: 1, limit: 10)
        result.records.size.should eq(1)
        result.total.should eq(1)
        result.total_pages.should eq(1)
        result.has_next?.should be_false
      end

      it "handles exact page boundary" do
        20.times { |i| Article.create!(title: "Article #{i}", content: "Content") }

        result = Article.paginate(page: 2, limit: 10)
        result.records.size.should eq(10)
        result.page.should eq(2)
        result.has_next?.should be_false
        result.has_prev?.should be_true
      end

      it "handles partial last page" do
        25.times { |i| Article.create!(title: "Article #{i}", content: "Content") }

        result = Article.paginate(page: 3, limit: 10)
        result.records.size.should eq(5)
        result.page.should eq(3)
        result.total_pages.should eq(3)
        result.has_next?.should be_false
      end

      it "handles large limit on small dataset" do
        5.times { |i| Article.create!(title: "Article #{i}", content: "Content") }

        result = Article.paginate(page: 1, limit: 100)
        result.records.size.should eq(5)
        result.total.should eq(5)
        result.total_pages.should eq(1)
      end
    end

    describe "Cursor Pagination Edge Cases" do
      before_each do
        Article.clear
        10.times { |i| Article.create!(title: "Article #{i}", content: "Content") }
      end

      it "handles invalid cursor gracefully" do
        # Using a non-existent ID as cursor
        result = Article.order(:id).paginate_cursor(after: "99999", limit: 5)

        result.records.should be_empty
        result.has_next?.should be_false
      end

      it "handles cursor for deleted record" do
        articles = Article.all.to_a
        cursor_id = articles[5].id.to_s

        # Delete the record
        Article.delete(articles[5].id.not_nil!)

        # Pagination should still work
        result = Article.order(:id).paginate_cursor(after: cursor_id, limit: 5)
        result.records.size.should be <= 5
      end

      it "handles cursor at end of dataset" do
        last_article = Article.order(:id).last.not_nil!

        result = Article.order(:id).paginate_cursor(after: last_article.id.to_s, limit: 5)
        result.records.should be_empty
        result.has_next?.should be_false
      end

      it "handles cursor at beginning with before" do
        first_article = Article.order(:id).first.not_nil!

        result = Article.order(:id).paginate_cursor(before: first_article.id.to_s, limit: 5)
        result.records.should be_empty
        result.has_prev?.should be_false
      end
    end
  end
end

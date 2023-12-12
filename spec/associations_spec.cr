require "./spec_helper"

describe PgORM::Associations do
  describe "belongs_to" do
    it "test assocation" do
      typeof(Book.new(id: 1, author_id: 1, name: "").author).should eq(Author)
      typeof(Account.new(id: 1, supplier_id: 1).supplier).should eq(Supplier)
    end

    it "test setter for new record" do
      book = Book.new
      book.author = Author.new
      book.author_id?.should be_nil
    end

    it "test setter" do
      author = Author.create(name: "Someone")
      book = Book.new(name: "Some Book")
      book.author = author

      author.id.should eq(book.author_id?)
    end

    it "test build" do
      book = Book.new
      author = book.build_author
      author.is_a?(Author).should be_true
      author.should eq(book.author)
      author.persisted?.should be_false
    end

    it "test create" do
      book = Book.new
      author = book.create_author(name: "Author")
      author.is_a?(Author).should be_true
      author.should eq(book.author)
      author.persisted?.should be_true
    end

    it "test autosave: nil" do
      book = Book.new(name: "Some Book")

      author = book.build_author(name: "Author")
      author.persisted?.should be_false

      book.save
      author.persisted?.should be_true

      author.name = "Another Author"
      book.save
      author.changed?.should be_true
    end

    it "test autosave: true" do
      book = BookAutosave.new(name: "Some Book")

      author = book.build_author(name: "Author")
      author.persisted?.should be_false

      book.save
      author.persisted?.should be_true

      author.name = "Another Author"
      book.save
      author.changed?.should be_false
    end

    it "test autosave: false" do
      book = BookNoAutosave.new(name: "Some Book")

      author = book.build_author(name: "Author")
      author.persisted?.should be_false

      expect_raises(PgORM::Error::RecordInvalid, "`author_id` should not be nil") do
        book.save!
      end
      book.persisted?.should be_false
      author.persisted?.should be_false
    end

    it "test dependent: nil" do
      account = Account.new
      account.supplier = Supplier.create
      account.save

      account.destroy
      account.supplier.destroyed?.should be_false

      account.supplier.reload!.should_not be_nil
    end

    it "test dependent: delete" do
      account = AccountDependentDelete.new
      account.supplier = Supplier.create
      account.save

      account.destroy
      account.supplier.destroyed?.should be_false

      expect_raises(PgORM::Error::RecordNotFound) do
        account.supplier.reload!
      end
    end

    it "test dependent: destroy" do
      account = AccountDependentDestroy.new
      account.supplier = Supplier.create
      account.save

      account.destroy
      account.supplier.destroyed?.should be_true

      expect_raises(PgORM::Error::RecordNotSaved) do
        account.supplier.reload!
      end
    end
  end

  describe "has_one" do
    it "test getter" do
      typeof(Supplier.new.account).should eq(Account)
    end

    it "test setter for new record" do
      supplier = Supplier.new
      supplier.account = Account.new
      supplier.account.persisted?.should be_false
    end

    it "test setter" do
      supplier = Supplier.create
      first, second = Account.new, Account.new

      supplier.account = first
      first.persisted?.should be_true
      supplier.id.should eq(first.supplier_id)

      supplier.account = second
      second.persisted?.should be_true
      supplier.id.should eq(second.supplier_id)
    end

    it "test setter with dependent: nullify" do
      supplier = SupplierDependentNullify.create
      first, second = Account.new, Account.new

      supplier.account = first
      supplier.account = second

      first.reload!.supplier_id.should be_nil
      supplier.id.should eq(second.supplier_id)
      supplier.id.should eq(second.reload!.supplier_id)
    end

    it "test setter with dependent: delete" do
      supplier = SupplierDependentDelete.create
      first, second = Account.new, Account.new

      supplier.account = first
      supplier.account = second

      first.destroyed?.should be_false

      expect_raises(PgORM::Error::RecordNotFound) do
        first.reload!
      end

      supplier.id.should eq(second.supplier_id)
      supplier.id.should eq(second.reload!.supplier_id)
    end

    it "test setter with dependent: delete" do
      supplier = SupplierDependentDestroy.create
      first, second = Account.new, Account.new

      supplier.account = first
      supplier.account = second

      first.destroyed?.should be_true

      expect_raises(PgORM::Error::RecordNotSaved) do
        first.reload!
      end

      supplier.id.should eq(second.supplier_id)
      supplier.id.should eq(second.reload!.supplier_id)
    end

    it "test build" do
      supplier = Supplier.create
      account = supplier.build_account
      account.is_a?(Account)
      account.should eq(supplier.account)
      account.persisted?.should be_false
    end

    it "test create" do
      supplier = Supplier.create
      account = supplier.create_account
      account.is_a?(Account)
      account.should eq(supplier.account)
      account.persisted?.should be_true
    end

    it "test autosave: nil" do
      supplier = Supplier.new

      account = supplier.build_account
      account.persisted?.should be_false

      supplier.save
      account.persisted?.should be_true
    end

    it "test autosave: true" do
      supplier = SupplierAutosave.new

      account = supplier.build_account
      account.persisted?.should be_false

      supplier.save
      account.persisted?.should be_true
    end

    it "test autosave: false" do
      supplier = SupplierNoAutosave.new

      account = supplier.build_account
      account.persisted?.should be_false

      supplier.save
      account.persisted?.should be_false
    end

    it "test dependent: nil" do
      supplier = Supplier.create
      account = supplier.create_account
      supplier.destroy
      account.reload!.should_not be_nil
    end

    it "test dependent: delete" do
      supplier = SupplierDependentDelete.create
      account = supplier.create_account
      supplier.destroy

      supplier.account.destroyed?.should be_false
      expect_raises(PgORM::Error::RecordNotFound) do
        account.reload!
      end
    end

    it "test dependent: destroy" do
      supplier = SupplierDependentDestroy.create
      account = supplier.create_account
      supplier.destroy

      supplier.account.destroyed?.should be_true
      expect_raises(PgORM::Error::RecordNotSaved) do
        account.reload!
      end
    end

    it "test dependent: nullify" do
      supplier = SupplierDependentNullify.create
      account = supplier.create_account
      supplier.destroy
      account.reload!.supplier_id.should be_nil
    end
  end

  describe "has_many" do
    it "test getter" do
      typeof(Author.new.books).should eq(PgORM::Relation(Book))
      expect_raises(PgORM::Error::RecordNotSaved) do
        Author.new(name: "").books.to_a
      end
    end

    it "test build" do
      author = Author.create(name: "Author")
      book = author.books.build(name: "Book")
      book.persisted?.should be_false
      book.id?.should be_nil
      author.id.should eq(book.author_id?)
    end

    it "test create" do
      author = Author.create(name: "Author")
      book = author.books.create(name: "Book")
      book.persisted?.should be_true
      book.id?.should_not be_nil
      author.id.should eq(book.author_id?)
    end

    it "test delete" do
      author = Author.create(name: "Author")
      book1 = author.books.create(name: "This is first book")
      book2 = author.books.create(name: "This is second book")
      book3 = author.books.create(name: "This is third book")

      # fill cache
      author.books.to_a
      author.books.size.should eq(3)

      # delete
      author.books.delete(book3, book1)

      # removed from db
      author.books.ids.should eq([book2.id])

      # removed from cache
      author.books.map(&.id).should eq([book2.id])
    end

    it "test autosave: nil" do
      author = Author.new(name: "Author")
      book1 = author.books.build(name: "Book1")
      book1.name = "My Book"

      author.save
      author.persisted?.should be_true

      book2 = author.books.create(name: "Book2")
      book2.name = "My another book"

      author.save

      book2.changed?.should be_true
      "Book2".should eq(book2.reload!.name)

      book1.changed?.should be_false
      "My Book".should eq(book1.reload!.name)
    end

    it "test join" do
      author = AuthorAutosave.new(name: "Author")
      author.save
      author.persisted?.should be_true

      book1 = author.books.create(name: "Book1")
      book1.name = "My Book"

      book2 = author.books.build(name: "Book2")
      book2.name = "My another book"
      author.save

      result = AuthorAutosave.where(id: author.id).join(:inner, BookAutosave, :author_id).to_a.first
      books = JSON.parse(result.to_json).as_h["books"]?
      books.should_not be_nil
      books.try &.size.should eq(2)
      book3 = author.books.build(name: "Book3")
      book3.name = "My 3rd book"
      author.save

      result = AuthorAutosave.where(id: author.id).join(:inner, BookAutosave, :author_id).to_a.first
      books = JSON.parse(result.to_json).as_h["books"]?
      books.should_not be_nil
      books.try &.size.should eq(3)
    end

    it "test autosave: true" do
      author = AuthorAutosave.new(name: "Author")
      author.save
      author.persisted?.should be_true

      book1 = author.books.create(name: "Book1")
      book1.name = "My Book"

      book2 = author.books.build(name: "Book2")
      book2.name = "My another book"

      author.save

      book1.changed?.should be_false
      "My Book".should eq(book1.reload!.name)

      book2.changed?.should be_false
      "My another book".should eq(book2.reload!.name)
    end

    it "test autosave: false" do
      author = AuthorNoAutosave.new(name: "Author")
      author.books.build(name: "Book1")
      author.books.build(name: "Book2")
      author.save

      author.persisted?.should be_true
      author.books.any?(&.persisted?).should be_false
      author.books.reload.size.should eq(0)
    end
  end
end

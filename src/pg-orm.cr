# PgORM - A type-safe, high-performance PostgreSQL ORM for Crystal.
#
# PgORM provides an ActiveRecord-style interface for working with PostgreSQL databases,
# with compile-time type safety and zero-cost abstractions.
#
# ## Features
#
# - **Type-safe queries**: All queries are type-checked at compile time
# - **ActiveRecord-style API**: Familiar interface for Rails developers
# - **High performance**: Zero runtime overhead, connection pooling, lazy loading
# - **Full-text search**: Native PostgreSQL FTS support with ranking
# - **Real-time changefeeds**: Subscribe to database changes via LISTEN/NOTIFY
# - **Advisory locks**: Distributed locking for critical sections
# - **Pagination**: Both offset-based and cursor-based pagination
# - **Associations**: belongs_to, has_one, has_many with autosave and dependent options
# - **Validations**: Built-in and custom validators
# - **Callbacks**: Lifecycle hooks for create, update, destroy
# - **Transactions**: Nested transaction support with savepoints
#
# ## Quick Start
#
# ```
# require "pg-orm"
#
# # Configure database
# PgORM::Database.configure do |settings|
#   settings.host = "localhost"
#   settings.db = "myapp"
#   settings.user = "postgres"
# end
#
# # Define a model
# class User < PgORM::Base
#   include PgORM::Timestamps
#
#   attribute id : Int64
#   attribute name : String
#   attribute email : String
#   attribute active : Bool = true
# end
#
# # Create records
# user = User.create!(name: "John", email: "john@example.com")
#
# # Query records
# users = User.where(active: true).order(:name).limit(10).to_a
#
# # Update records
# user.update!(name: "Jane")
#
# # Delete records
# user.destroy
# ```
#
# ## Associations
#
# ```
# class Author < PgORM::Base
#   attribute id : Int64
#   attribute name : String
#   has_many :books
# end
#
# class Book < PgORM::Base
#   attribute id : Int64
#   attribute author_id : Int64
#   attribute title : String
#   belongs_to :author
# end
#
# author = Author.find(1)
# author.books.each { |book| puts book.title }
# ```
#
# ## Full-Text Search
#
# ```
# # Simple search
# Article.search("crystal programming", :title, :content)
#
# # Weighted search (title more important)
# Article.search_weighted("crystal", {
#   title:   FullTextSearch::Weight::A,
#   content: FullTextSearch::Weight::B,
# })
#
# # Ranked search (ordered by relevance)
# Article.search_ranked("crystal", :title, :content)
# ```
#
# ## Pagination
#
# ```
# # Offset-based pagination
# result = User.paginate(page: 2, limit: 20)
# result.records.each { |user| puts user.name }
# puts "Page #{result.page} of #{result.total_pages}"
#
# # Cursor-based pagination (more efficient)
# result = User.order(:id).paginate_cursor(limit: 20)
# next_result = User.order(:id).paginate_cursor(after: result.next_cursor, limit: 20)
# ```
#
# ## Changefeeds (Real-time Updates)
#
# ```
# # Subscribe to changes
# User.changes.on do |change|
#   case change.event
#   when .created?
#     puts "New user: #{change.value.name}"
#   when .updated?
#     puts "Updated user: #{change.value.name}"
#   when .deleted?
#     puts "Deleted user: #{change.value.id}"
#   end
# end
# ```
module PgORM
  VERSION = {{ `shards version "#{__DIR__}"`.chomp.stringify.downcase }}
end

require "./pg-orm/**"

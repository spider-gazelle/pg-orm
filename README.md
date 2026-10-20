# Postgres ORM for Crystal Lang

Extending [ActiveModel](https://github.com/spider-gazelle/active-model) for attribute definitions, callbacks and validations

- [Postgres ORM for Crystal Lang](#postgres-orm-for-crystal-lang)
  - [Configuration](#configuration)
  - [Primary Key](#primary-key)
  - [Table Name](#table-name)
  - [Callbacks](#callbacks)
  - [Associations](#associations)
    - [`belongs_to`](#belongs_to)
    - [`has_one`](#has_one)
    - [`has_many`](#has_many)
    - [Dependency](#dependency)
  - [Changefeeds](#changefeeds)
  - [Advisory Locks](#advisory-locks)
  - [Column Types](#column-types)
  - [Validations](#validations)
    - [`ensure_unique`](#ensure_unique)
  - [Timestamps](#timestamps)
  - [Query DSL Methods](#query-dsl-methods)
    - [OR Queries](#or-queries)
    - [Pattern Matching](#pattern-matching)
    - [Comparison Operators](#comparison-operators)
    - [Range Queries](#range-queries)
    - [Combining Query Methods](#combining-query-methods-1)
  - [Full-Text Search](#full-text-search)
    - [Basic Search](#basic-search)
    - [Ranked Search](#ranked-search)
    - [Weighted Search](#weighted-search)
    - [Advanced Search Methods](#advanced-search-methods)
    - [Pre-computed tsvector Columns](#pre-computed-tsvector-columns)
    - [Text Search Configuration](#text-search-configuration)
    - [Combining with Query Methods](#combining-with-query-methods)
    - [Query Optimization](#query-optimization)
  - [Pagination](#pagination)
    - [Page-based Pagination](#page-based-pagination)
    - [Offset-based Pagination](#offset-based-pagination)
    - [Cursor-based Pagination](#cursor-based-pagination)
    - [Memory Efficiency](#memory-efficiency)
    - [Pagination with Joins](#pagination-with-joins)
    - [JSON Serialization](#json-serialization)
    - [Pagination with Full-Text Search](#pagination-with-full-text-search)
  - [Join](#join)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Testing](#testing)
    - [With Docker](#with-docker)
    - [Without Docker](#without-docker)
  - [Compiling](#compiling)
  - [Benchmark](#benchmark)
    - [Results](#results)

## Configuration

```crystal
# Below is the list of settings exposed by PgORM and their defaults
PgORM::Database.configure do |settings|
  setting host : String = ENV["PG_HOST"]? || "localhost"
  setting port : Int32 = (ENV["PG_PORT"]? || 5432).to_i
  setting db : String = ENV["PG_DB"]? || ENV["PG_DATABASE"]? || "test"
  setting user : String = ENV["PG_USER"]? || "postgres"
  setting password : String = ENV["PG_PASSWORD"]? || ""
  setting query : String = ENV["PG_QUERY"]? || ""
  # Optional read replica host (see Read replicas below)
  setting host_read_only : String? = ENV["PG_RO_HOST"]?
  setting query_read_only : String = ENV["PG_RO_QUERY"]? || ENV["PG_QUERY"]? || ""
  # Postgresql Advisory Lock wait time-out
  setting lock_timeout : Time::Span = (ENV["PG_LOCK_TIMEOUT"]? || 5).to_i.seconds
end

# OR

PgORM::Database.parse(ENV["PG_DATABASE_URL"])
```

### Read replicas

PgORM can optionally route read-only queries to a separate connection (e.g. a DNS
entry fronting one or more PostgreSQL read replicas). Configure it with a single
connection URL (passing `nil` or a blank string disables replica routing):

```crystal
PgORM::Database.parse_read(ENV["PG_DATABASE_READ_URL"]?)
```

Alternatively, set the `host_read_only` setting (`PG_RO_HOST`). The replica URL is
then built from the primary's user, password, port and database, plus
`query_read_only`, when the primary URL is first built from the settings (it is
not used when the primary is configured with `parse`).

When configured, standalone `SELECT` queries (`find`, `where`, `count`,
`find_*_by_sql`, etc.) are sent to the replica, while **all writes, and any reads
issued inside a `transaction` or `with_connection` block, use the primary**, so
reads inside those blocks see the block's own writes. A standalone read issued
after a write is sent to the replica and may not see that write until it has
replicated. Change feeds and advisory locks always use the primary. If no replica
is configured, every query uses the primary.

To force a read onto the primary (e.g. immediately after a write that may not have
replicated yet), wrap it in `with_connection`:

```crystal
PgORM::Database.with_connection do
  user.save!
  User.find(user.id) # reads the primary, not the replica
end
```

## Primary Key

The primary key defaults to a column named `id`, but no attribute is generated for it:
every model must declare its primary key attribute(s), otherwise compilation fails with
`primary keys in Foo must be defined as an attribute`.

```crystal
class Foo < PgORM::Base
  attribute id : Int64
  attribute name : String
end
```

A non-nilable `id` attribute is treated as database generated, so it is not required to
be set before saving. A primary key left unset is omitted from the `INSERT` and the
generated value is read back after creation.

The `default_primary_key(name, auto_generated = true, converter = nil, **tags)` macro
declares the primary key and its attribute in one step. The attribute is created with
`mass_assignment: false`, and `auto_generated` (default `true`) marks it as generated by
the database.

```crystal
class Foo < PgORM::Base
  default_primary_key id : Int64

  attribute name : String
  attribute foo : Hash(String, String)
  attribute baz : Array(Float64)
end
```

Declared on an abstract base class, `default_primary_key` makes subclasses inherit the
attribute, but each subclass resets its primary key name to `id`. If the key is not named
`id`, repeat `primary_key` in every subclass:

```crystal
abstract class BaseModel < PgORM::Base
  default_primary_key my_pk : String
end

class Foo < BaseModel
  primary_key :my_pk

  attribute name : String
end
```

If your models have different primary key, you can specify it using the primary_key directive.

```crystal
class Bar < PgORM::Base
  primary_key :uuid

  attribute uuid : UUID
end

```

for composite keys

```crystal
class UserOrganisation < PgORM::Base
  primary_key :user_id, :org_id

  attribute user_id : UUID
  attribute org_id : UUID
end

```

## Table Name

Table name is inferred from the class name if none is provided: the class name underscored, with `::` replaced by `_` and no pluralisation (e.g. `Article` uses the table `article`). You can override this behavior via `table` macro

```crystal
class Baz < PgORM::Base
  table "awsome_table"

  # id is the default primary key
  attribute id : Int32
  attribute name : String
  attribute about : String? = nil
end
```

## Callbacks

Register callbacks for `save`, `update`, `create` and `destroy` by setting the corresponding before/after callback handler.

```crystal
class ModelWithCallbacks < PgORM::Base
  attribute id : Int32
  attribute name : String
  attribute address : String
  attribute age : Int32 = 10

  before_create :set_address
  after_update :set_age

  before_destroy do
    self.name = "joe"
  end

  def set_address
    self.address = "23"
  end

  def set_age
    self.age = 30
  end
end
```

## Associations

Set associations with `belongs_to`, `has_one`, and `has_many`.

Access children in parent by accessing the method correpsonding to the name.

> **Note:** The association macros don't declare foreign key attributes. Declare the
> foreign key attribute (e.g. `attribute parent_id : Int64?`) on the model that holds it:
> the `belongs_to` model, or the child model of a `has_one`/`has_many`.

```crystal
class Parent < PgORM::Base
  attribute id : Int64
  attribute name : String
  has_many :children, class_name: Child
end

class Child < PgORM::Base
  attribute id : Int64
  attribute age : Int32
  attribute parent_id : Int64?
  belongs_to :parent
  has_many :pets
end

class Pet < PgORM::Base
  attribute id : Int64
  attribute name : String
  attribute child_id : Int64?
  belongs_to :child
end

parent = Parent.new(name: "Phil")
parent.children.to_a.empty? # => true

child = Child.new(age: 99)
child.pets.to_a.empty? # => true
```

### `belongs_to`

This will add the following methods:

> **Note**: `association` below refers to the name parameter provided when defining this association, e.g `belongs_to :child` here `child` is the association name):

- `association` returns the associated object (raises `PgORM::Error::RecordNotFound` if it doesn't exist);
- `association?` returns the associated object, or nil if it doesn't exist (both
  methods raise `NilAssertionError` if the foreign key is `nil`);
- `association=` assigns the associated object, assigning the foreign key if the object is persisted;
- `build_association` builds the associated object, assigning the foreign
  key if the parent record is persisted, or delaying it to when the new
  record is saved;
- `create_association` creates the associated object with `create` and assigns the
  foreign key;
- `create_association!` same as `create_association` but uses `create!`.
  Both raise `PgORM::Error::RecordInvalid` when validation fails;
- `reload_association` to reload the associated object.

For example a Child class declares `belongs_to :parent` which will add:

- `Child#parent` (similar to `Parent.find(parent_id)`)
- `Child#parent=(parent)` (similar to `child.parent_id = parent.id`)
- `Child#build_parent` (similar to child.parent = Parent.new)
- `Child#create_parent` (similar to child.parent = Parent.create)
- `Child#create_parent!` (similar to child.parent = Parent.create!)
- `Child#reload_parent` (force reload child.parent)

| Parameter     |                                                                                                                                                                                                                                                                             | Default          |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| `name`        | Name of the association                                                                                                                                                                                                                                                     |                  |
| `class_name`  | overrides the association class name (inferred from `name` by default)                                                                                                                                                                                                      | `name.camelcase` |
| `foreign_key` | overrides the foreign key attribute on this model                                                                                                                                                                                                                           | `name + "_id"`   |
| `autosave`    | Set auto save behavior. One of `nil`, `true`, `false` . Set `nil` (default) to only save newly built associations when the parent record is saved, `true` to always save the associations (new or already persisted), `false` to never save the associations automatically. | `nil`            |
| `dependent`   | Sets destroy behaviour. One of `nil`, `:delete`, `:destroy`. Set `nil` when no deletion should occur. `:delete` to delete associated record in SQL, `:destroy` to call `#destroy` on the associated object.                                                                 | `nil`            |

### `has_one`

Declares a has one relationship.

This will add the following methods:

- `association` returns the associated object (raises `PgORM::Error::RecordNotFound` if it doesn't exist);
- `association?` returns the associated object (or nil);
- `association=` assigns the associated object. If this record is persisted, the
  previously associated record is deleted (`dependent: :delete`), destroyed
  (`dependent: :destroy`) or otherwise has its foreign key set to `nil`, then the new
  object's foreign key is assigned and it is saved;
- `build_association` builds the associated object, assigning its foreign key if this
  record is persisted;
- `create_association` builds and saves (`save`) the associated object;
- `create_association!` builds and saves (`save!`) the associated object, raising
  `PgORM::Error::RecordInvalid` when validation fails. Both raise
  `PgORM::Error::RecordNotSaved` if this record has no id;
- `reload_association` to reload the associated object.

For example an Account class declares `has_one :supplier` which will add:

- `Account#supplier` (similar to `Supplier.find_by(account_id: account.id)`)
- `Account#supplier=(supplier)` (similar to `supplier.account_id = account.id`)
- `Account#build_supplier`
- `Account#create_supplier`
- `Account#create_supplier!`
- `Account#reload_supplier`

| Parameter     |                                                                                                                                                                                                                                                                             | Default                                |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `name`        | Name of the association                                                                                                                                                                                                                                                     |                                        |
| `class_name`  | overrides the association class name (inferred from `name` by default)                                                                                                                                                                                                      | `name.camelcase`                       |
| `foreign_key` | overrides the foreign key attribute on the associated model                                                                                                                                                                                                                 | this model's name, underscored + `_id` |
| `autosave`    | Set auto save behavior. One of `nil`, `true`, `false` . Set `nil` (default) to only save newly built associations when the parent record is saved, `true` to always save the associations (new or already persisted), `false` to never save the associations automatically. | `nil`                                  |
| `dependent`   | Sets destroy behaviour. One of `nil`, `:nullify`, `:delete`, `:destroy`. Set `nil` when nothing should happen, `:nullify` to set the foreign key `nil` in SQL, `:delete` to delete associated record in SQL, `:destroy` to call `#destroy` on the associated object.        | `nil`                                  |

### `has_many`

Declares a has many relationship.

This will add method

- `association` returns `Relation` object of the associated object.

For example a Parent class declares `has_many :children, class_name: Child` which will add:

- `Parent#children : Relation(Child)` (similar to `Child.where(parent_id: parent.id)`)

| Parameter     |                                                                                                                                                                                                                                                                                                          | Default                                                     |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| `name`        | Name of the association                                                                                                                                                                                                                                                                                  |                                                             |
| `class_name`  | overrides the association class name                                                                                                                                                                                                                                                                     | `name` with a trailing `s` removed, camelcased              |
| `foreign_key` | overrides the foreign key attribute on the associated model                                                                                                                                                                                                                                              | this model's name (trailing `s` removed), underscored + `_id` |
| `autosave`    | Set auto save behavior for loaded or built records. One of `nil`, `true`, `false` . Set `nil` (default) to only save newly built associations when the parent record is saved, `true` to always save the associations (new or already persisted), `false` to never save the associations automatically. | `nil`                                                       |
| `dependent`   | Sets destroy behaviour. One of `nil`, `:nullify`, `:delete_all`, `:destroy`. Set `nil` when nothing should happen, `:nullify` to set the foreign key `nil` in SQL, `:delete_all` to delete associated records in SQL, `:destroy` to call `#destroy` on each associated object.                           | `nil`                                                       |
| `serialize`   | When `true` will add linked attribute to `to_json` representation, if the associated records have been loaded (e.g. by a join)                                                                                                                                                                           | `false`                                                     |

### Dependency

`dependent` param in the association definition macros defines the fate of the association on model destruction. It defaults to `nil` (do nothing) for all three macros. Refer to descriptions in specific association for the accepted values; any other value (e.g. `:delete` on `has_many`) is silently ignored.

## Changefeeds

Access the changefeed (CRUD Events) of a table through the `changes` class method.<br>

Defaults to watch for change events on a table if no id provided.

| Parameter |                                                                     | Default |
| --------- | ------------------------------------------------------------------- | ------- |
| `id`      | id of record to watch for changes or `nil` to watch for whole table | nil     |

Returns a `ChangeFeed` instance which provides methods for **event based** or **blocking iterator**<br>

- `ChangeFeed#on` expects a block to be passed, which will get invoked asynchronously when an event is received.
- `ChangeFeed#each` an `Iterator`, whose `next` call will block till an event is received.<br>

Emits `Change` instance consisting of `event : Event` and `value : T` where `T` is the model.

Events:

- `ChangeReceiver::Event::Deleted` events yield the deleted model
- `ChangeReceiver::Event::Created` events yield the created model
- `ChangeReceiver::Event::Updated` events yield the updated model

```crystal
class Game < PgORM::Base
  attribute id : Int64
  attribute type : String
  attribute score : Int32 = 0
end

ballgame = Game.create!(type: "footy")

# Observe changes on a single row
spawn do
  Game.changes(ballgame.id).each do |change|
    game = change.value
    puts "looks like the score is #{game.score}"
  end
end

# Observe changes on a table
spawn do
  Game.changes.each do |change|
    game = change.value
    puts "#{game.type}: #{game.score}"
    puts "game event: #{change.event}"
  end
end
```

### Ignoring telemetry-only updates

Configure update filtering on the model:

```crystal
class Display < PgORM::Base
  attribute id : Int64
  attribute name : String
  attribute last_seen : Time?
  attribute current_item_id : String?

  changefeed_ignore_updates :last_seen, :current_item_id
end

feed = Display.changes
```

Registration passes the declared fields to EventBus, which installs a SQL trigger condition. Updates changing only these fields are persisted without producing changefeed events. An update changing any other field still emits its normal full payload; INSERT and DELETE are unchanged. No-op updates are silent on filtered tables.

Positional names must be persisted model attributes; `id` cannot be ignored. These are database attribute names, not JSON aliases. Filtering applies to every writer of the table, not only ORM saves. An ORM save that also changes `updated_at` still emits an event unless that field is explicitly included; choose ignored fields carefully so configuration changes remain visible.

For columns that exist only in PostgreSQL, such as a generated search vector, declare an explicit `database_columns` list:

```crystal
changefeed_ignore_updates :name, :description, :updated_at,
  database_columns: [:search_vector]
```

Positional names still require persisted model attributes. `database_columns` accepts an array of column symbols; EventBus verifies they exist when the changefeed is registered. Neither list can include `id`. The lists form one policy and are overridden together by a subclass declaration. Generated columns may change with their source fields, so include the derived column when ignoring those source changes; updates to other source columns still notify.

A model without a declaration passes no policy, preserving any existing table policy. Declarations on abstract model bases are inherited and can be overridden by subclasses; sibling models keep their own configuration. An empty declaration requests an unfiltered policy and conflicts with an existing filtered policy rather than silently clearing it.

Policies are shared across services using the same table. Registering a model whose declaration differs from the installed policy replaces it, so changing `changefeed_ignore_updates` takes effect on the next deploy; keep declarations consistent across services sharing a table. Upgrade every service that installs EventBus triggers before enabling filtering, and register the changefeed before starting telemetry writers if the first write must be silent.

## Advisory Locks

`PgORM::PgAdvisoryLock` class provides a means for creating PostgreSQL [Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS).

```crystal
 lock = PgORM::PgAdvisoryLock.new("name or label to uniquely identify this lock")
 lock.synchronize do
   # Do some work
 end

 # OR if you need control on when to release the lock
 lock.lock
 # do some work
 # some more work
 lock.unlock
```

## Column Types

Shard doesn't impose any restrictions on the types used in attributes and you are free to use any of the standard library or custom data types. For complex or custom data types, you are provided with an option to either provide custom `converter` which will be invoked when reading and writing to the table or shard assumes your complex data type supports JSON serialization method and field in stored in Postgres as **JSONB** data type.

Below is a list of several Crystal type that shard maps to Postgres column types

| Crystal Type       | Postgres column Type                    |
| ------------------ | --------------------------------------- |
| String             | TEXT                                    |
| Int16              | SMALLINT                                |
| Int32              | INTEGER                                 |
| Int64              | BIGINT                                  |
| Float64            | NUMERIC                                 |
| Bool               | BOOLEAN                                 |
| Time               | TIMESTAMP with time zone (TIMESTAMPTZ)  |
| UUID               | UUID                                    |
| JSON::Any          | JSONB                                   |
| JSON::Serializable | JSONB                                   |
| Array(T)           | [] where T is any other supported type. |
| Enum               | INTEGER                                 |
| Set(T)             | [] where T is any other supported type  |
| Custom type        | JSONB                                   |

Any of your columns can also define “nilable” types by adding Crystal Nil Union `?`. This is to let shard knows that your database table column allows for a NULL value.

## Validations

Builds on [active-model's validation](https://github.com/spider-gazelle/active-model#validations)

### `ensure_unique`

Fails to validate if field with duplicate value present in db.
If scope is set, the callback/block signature must be a tuple with types matching that of the scope.
On new records, the field(s) are set with the result of the transform block upon successful validation

| Parameter           |                                                         | Default   |
| ------------------- | ------------------------------------------------------- | --------- |
| `field`             | Model attribute on which to guarantee uniqueness        |           |
| `scope`             | Attributes passed to the transform, defaults to :field  | `[field]` |
| `callback : T -> T` | Optional method to transform field value                | nil       |
| `block : T -> T`    | Optional block to transform field value before querying | nil       |

## Timestamps

Adds `created_at` and `updated_at` attributes.

- `created_at` and `updated_at` are both set through the `before_create` callback.
- `updated_at` is set through the `before_update` callback.

The generated timestamp is UTC.

```crystal
class Timo < PgORM::Base
  # Simply include the module
  include PgORM::Timestamps

  attribute id : Int64
  attribute name : String
end
```

## Query DSL Methods

PgORM provides a comprehensive set of ActiveRecord-style query methods for building complex database queries.

### OR Queries

Combine two query scopes with OR logic (ActiveRecord-style):

```crystal
# Basic OR
User.where(name: "John").or(User.where(name: "Jane"))
# => WHERE (name = 'John') OR (name = 'Jane')

# Complex OR with multiple conditions
User.where(active: true, role: "admin")
    .or(User.where(active: true, role: "moderator"))
# => WHERE (active = true AND role = 'admin') OR (active = true AND role = 'moderator')

# Chain with other query methods
User.where(name: "Alice")
    .or(User.where(name: "Bob"))
    .order(:name)
    .limit(10)
```

### Pattern Matching

#### LIKE (Case-Sensitive)

```crystal
# Suffix match
User.where_like(:email, "%@example.com")

# Prefix match
User.where_like(:name, "John%")

# Contains
Article.where_like(:domain, "%example%")

# Negation
User.where_not_like(:email, "%@spam.com")
```

#### ILIKE (Case-Insensitive)

```crystal
# Case-insensitive matching
User.where_ilike(:email, "%@EXAMPLE.com")

# Domain partial matching (solves the tsvector limitation)
Article.where_ilike(:domain, "%example%")
# Matches: "api.example.com", "example.org", "test.EXAMPLE.net"

# Negation
User.where_not_ilike(:name, "%test%")
```

### Comparison Operators

```crystal
# Greater than
User.where_gt(:age, 18)

# Greater than or equal
User.where_gte(:age, 18)

# Less than
User.where_lt(:age, 65)

# Less than or equal
User.where_lte(:age, 65)

# Chaining comparisons
User.where_gte(:age, 18).where_lt(:age, 65)
```

### Range Queries

```crystal
# BETWEEN (inclusive)
User.where_between(:age, 18, 65)
# => WHERE age BETWEEN 18 AND 65

# Works with Time values
Article.where_between(:created_at, 1.week.ago, Time.utc)

# NOT BETWEEN
User.where_not_between(:age, 18, 65)
```

### Combining Query Methods

All query methods can be chained together:

```crystal
# Complex query with multiple DSL methods
Article.where(published: true)
       .where_ilike(:domain, "%example%")
       .where_gte(:views, 100)
       .or(Article.where(featured: true))
       .order(created_at: :desc)
       .limit(20)

# Pattern matching with OR
User.where_like(:email, "%@gmail.com")
    .or(User.where_like(:email, "%@yahoo.com"))

# Range with other conditions
Article.where(category: "tech")
       .where_between(:created_at, 1.month.ago, Time.utc)
       .where_gt(:views, 1000)
```

## Full-Text Search

PgORM provides comprehensive full-text search capabilities using PostgreSQL's `tsvector` and `tsquery` features.

### Basic Search

Search across one or more columns using PostgreSQL's text search.

`search`, `search_ranked`, `search_weighted`, `search_ranked_weighted`, `search_vector` and
`search_vector_ranked` pass the query to `to_tsquery`, so it must be valid tsquery syntax:
join words with operators such as `&` or `|`. Plain multi-word input such as
`"crystal programming"` raises a PostgreSQL syntax error. For user-entered text use
`search_plain` or `search_vector_plain`, which use `plainto_tsquery`.

```crystal
class Article < PgORM::Base
  table "articles"

  attribute id : Int64
  attribute title : String
  attribute content : String
end

# Basic search with symbols (developer-friendly)
Article.search("crystal", :title, :content)

# Basic search with strings
Article.search("crystal", "title", "content")

# Array of columns (e.g., from user selection)
columns = ["title", "content"]
Article.search("crystal", columns)

# Boolean operators
Article.search("crystal & programming", :title, :content)  # AND
Article.search("crystal | ruby", :title, :content)         # OR
Article.search("crystal & !ruby", :title, :content)        # NOT
```

### Ranked Search

Order results by relevance using `ts_rank` or `ts_rank_cd`:

```crystal
# Basic ranking (ts_rank)
Article.search_ranked("crystal & programming", :title, :content)

# Cover density ranking (ts_rank_cd)
Article.search_ranked("crystal", :title,
  rank_function: PgORM::FullTextSearch::RankFunction::RankCD)

# Rank normalization (divide by document length)
Article.search_ranked("crystal", :title, :content, rank_normalization: 1)
```

### Weighted Search

Assign different weights to columns (A=1.0, B=0.4, C=0.2, D=0.1):

```crystal
# With symbols
weights = {
  :title   => PgORM::FullTextSearch::Weight::A,
  :content => PgORM::FullTextSearch::Weight::B,
}
Article.search_weighted("crystal", weights)

# With strings (useful for frontend input)
weights = {
  "title"   => PgORM::FullTextSearch::Weight::A,
  "content" => PgORM::FullTextSearch::Weight::B,
}
Article.search_weighted("crystal", weights)

# Weighted search with ranking
Article.search_ranked_weighted("crystal", weights)
```

### Advanced Search Methods

```crystal
# Phrase search (exact phrase matching)
Article.search_phrase("crystal programming language", :content)

# Proximity search ("programming" exactly 5 positions after "crystal", tsquery `<5>`)
Article.search_proximity("crystal", "programming", 5, :content)

# Prefix search (matches crystal, crystalline, etc.)
Article.search_prefix("cryst", :title, :content)

# Plain text search (plainto_tsquery: words are ANDed, operators are ignored;
# use this for user input)
Article.search_plain("crystal programming", :title, :content)
```

### Pre-computed tsvector Columns

For production use, create a `tsvector` column with a GIN index for better performance:

```sql
-- Add tsvector column
ALTER TABLE articles ADD COLUMN search_vector tsvector;

-- Create GIN index
CREATE INDEX articles_search_idx ON articles USING GIN(search_vector);

-- Auto-update trigger
CREATE TRIGGER articles_search_update
  BEFORE INSERT OR UPDATE ON articles
  FOR EACH ROW EXECUTE FUNCTION
  tsvector_update_trigger(search_vector, 'pg_catalog.english', title, content);
```

Then search using the pre-computed column:

```crystal
# Basic search on tsvector column (symbol or string)
Article.search_vector("crystal & programming", :search_vector)
Article.search_vector("crystal & programming", "search_vector")

# Ranked search on tsvector column
Article.search_vector_ranked("crystal", :search_vector)

# Plain text search on tsvector column
Article.search_vector_plain("crystal programming", :search_vector)
```

### Text Search Configuration

Specify language configuration for stemming and stop words:

```crystal
# English (default)
Article.search("running", :content, config: "english")  # matches "run", "runs"

# Simple (no stemming)
Article.search("running", :content, config: "simple")   # exact match only
```

### Combining with Query Methods

Full-text search integrates seamlessly with other query methods:

```crystal
Article
  .where(published: true)
  .search("crystal", :title, :content)
  .limit(10)
  .order(:created_at)
```

### Query Optimization

Use the `explain` method to analyze query performance:

```crystal
query = Article.search_ranked("crystal", :title, :content)
puts query.explain  # Shows PostgreSQL query execution plan
```

## Pagination

PgORM provides comprehensive pagination support with three strategies: page-based, offset-based, and cursor-based pagination. All three default to `limit: 25`.

`paginate` and `paginate_by_offset` run a `COUNT` query immediately and load the page's records lazily, on first access. `paginate_cursor` is not lazy: it loads the page's records immediately (fetching `limit + 1` rows to detect whether there is a next page).

### Page-based Pagination

Standard page number pagination with complete metadata:

```crystal
# Basic pagination
result = Article.where(published: true).paginate(page: 2, limit: 20)

# Access metadata (the COUNT query has run, no records loaded yet)
result.total        # => 150
result.page         # => 2
result.total_pages  # => 8
result.has_next?    # => true
result.has_prev?    # => true
result.next_page    # => 3
result.prev_page    # => 1
result.from         # => 21
result.to           # => 40

# Load records (lazy - only loaded when accessed)
articles = result.records

# Works with all query methods
Article.where(published: true)
  .order(created_at: :desc)
  .paginate(page: 1, limit: 10)
```

### Offset-based Pagination

Direct control over offset and limit:

```crystal
# Paginate with offset
result = Article.paginate_by_offset(offset: 40, limit: 20)

result.offset  # => 40
result.limit   # => 20
result.page    # => 3 (calculated as offset / limit + 1)
```

### Cursor-based Pagination

Efficient pagination for large datasets using cursor-based navigation:

```crystal
# First page
result = Article.order(:id).paginate_cursor(limit: 20)

result.records       # => Array of 20 articles
result.has_next?     # => true
result.next_cursor   # => "123" (ID of last record)

# Next page using cursor
next_result = Article.order(:id)
  .paginate_cursor(after: result.next_cursor, limit: 20)

# Previous page using cursor
prev_result = Article.order(:id)
  .paginate_cursor(before: next_result.prev_cursor, limit: 20)
```

`before:` adds `cursor_column < before` to the query and keeps its order and limit, so with
an ascending order it returns the first `limit` matching rows, which is the immediately
preceding page only when it starts at the beginning of the result set.

`next_cursor` and `prev_cursor` always hold the primary key (`id`) of the boundary records,
even when `cursor_column` is set to another column. With a custom `cursor_column`, build the
cursor from that column yourself:

```crystal
result = Article.order(:created_at)
  .paginate_cursor(limit: 20, cursor_column: :created_at)

if last = result.records.last?
  Article.order(:created_at)
    .paginate_cursor(after: last.created_at.to_rfc3339(fraction_digits: 6), limit: 20, cursor_column: :created_at)
end
```

### Memory Efficiency

`paginate` and `paginate_by_offset` load records lazily, and at most `limit` records are
loaded at once:

```crystal
# Create pagination result (runs the COUNT query only)
result = Article.paginate(page: 1, limit: 1000)

# Check metadata without loading records
if result.total_pages >= 5
  # Records only loaded when accessed
  articles = result.records
end

# Iterating loads the whole page into memory, then yields each record
result.each do |article|
  process(article)
end
```

### Pagination with Joins

Automatically handles joined queries with correct counting:

```crystal
# Correctly counts distinct records in joins
result = Author.join(:left, Book, :author_id)
  .where("books.published = ?", true)
  .paginate(page: 1, limit: 10)

# Uses COUNT(DISTINCT authors.id) to avoid duplicate counts
result.total  # => 25 (distinct authors, not total join results)
```

### JSON Serialization

Pagination results include complete metadata in JSON:

```crystal
result = Article.where(published: true).paginate(page: 2, limit: 10)

result.to_json
# {
#   "data": [...],
#   "pagination": {
#     "total": 150,
#     "limit": 10,
#     "offset": 10,
#     "page": 2,
#     "total_pages": 15,
#     "has_next": true,
#     "has_prev": true,
#     "next_page": 3,
#     "prev_page": 1,
#     "from": 11,
#     "to": 20
#   }
# }
```

Cursor pagination JSON:

```crystal
result = Article.order(:id).paginate_cursor(limit: 10)

result.to_json
# {
#   "data": [...],
#   "pagination": {
#     "limit": 10,
#     "has_next": true,
#     "has_prev": false,
#     "next_cursor": "123",
#     "prev_cursor": null
#   }
# }
```

### Pagination with Full-Text Search

Pagination works seamlessly with all search methods:

```crystal
# Paginate search results
Article.search("crystal", :title, :content)
  .paginate(page: 1, limit: 20)

# Paginate ranked search
Article.search_ranked("crystal & programming", :title, :content)
  .paginate(page: 1, limit: 20)

# Paginate with filters
Article.where(published: true)
  .search("crystal", :title, :content)
  .paginate(page: 1, limit: 20)
```

## Join

Supports:

- **INNER JOIN**: Returns records that have matching values in both tables
- **LEFT JOIN**: Returns all records from the left table, and the matched records from the right table
- **RIGHT JOIN**: Returns all records from the right table, and the matched records from the left table
- **FULL JOIN**: Returns all records when there is a match in either left or right table

When a join SQL is performed, the joined records will be cached on each parent record and accessing the matching `has_many` association will use the cached result instead of hitting the database. `belongs_to` and `has_one` associations don't use join results.

> Default behavior of associations is to perform a lazy load. So linked associations aren't fetched unless accessed.

```crystal

class Parent < PgORM::Base
  attribute id : Int64
  attribute name : String
  has_many :children, class_name: Child, serialize: true # serialize tag will serialize `children` when `to_json` is invoked on parent object
end

class Child < PgORM::Base
  attribute id : Int64
  attribute age : Int32
  attribute parent_id : Int64?
  belongs_to :parent
  has_many :pets
end

class Pet < PgORM::Base
  attribute id : Int64
  attribute name : String
  attribute child_id : Int64?
  belongs_to :child
end

parent = Parent.new(name: "Phil")
parent.save!

child1 = parent.children.create(age: 6)
child2 = parent.children.create(age: 3)

parent.save!

result = Parent.where(id: parent.id).join(:left, Child, :parent_id).to_a.first
children = JSON.parse(result.to_json).as_h["children"]?
children.should_not be_nil
children.try &.size.should eq(2)
```

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     pg-orm:
       github: spider-gazelle/pg-orm
   ```

2. Run `shards install`

## Usage

```crystal
require "pg-orm"
```

## Testing

Given you have the following dependencies...

- [docker](https://www.docker.com/)
- [docker-compose](https://github.com/docker/compose)

It is simple to develop the service with docker.

### With Docker

- Run specs, tearing down the `docker-compose` environment upon completion.

```shell-session
$ ./test
```

- Run specs on changes to Crystal files within the `src` and `spec` folders.

```shell-session
$ ./test --watch
```

### Without Docker

- To run tests

```shell-session
$ crystal spec
```

**NOTE:** The upstream dependencies specified in `docker-compose.yml` are required...

## Compiling

```shell-session
$ shards build
```

## Benchmark

Using script from [Benchmark different ORMs for crystal and postgres](https://github.com/jwoertink/crystal_orm_test) , modified to add `PgORM` to the suite.

### Results

Specs:

```
Machine: Apple MBP M1 Max 32GB RAM
OS: macOS 15.2
Crystal 1.14.0
PG: 17.2
DATE: 2024-12-20
```

```
BENCHMARKING simple_insert
                             user     system      total        real
Avram simple_insert      0.041759   0.072528   0.114287 (  0.955047)
Crecto simple_insert     0.029846   0.026625   0.056471 (  0.427655)
Granite simple_insert    0.017180   0.026954   0.044134 (  0.413282)
Jennifer simple_insert   0.033598   0.071590   0.105188 (  0.937873)
PgORM simple_insert      0.026143   0.068314   0.094457 (  0.916158)
BENCHMARKING simple_select
                             user     system      total        real
Avram simple_select      0.721817   0.090855   0.812672 (  2.037978)
Crecto simple_select     0.817780   0.095371   0.913151 (  1.754842)
Granite simple_select    0.652766   0.079596   0.732362 (  1.562638)
Jennifer simple_select   0.515536   0.075688   0.591224 (  1.363594)
PgORM simple_select      0.187846   0.045833   0.233679 (  0.867318)
BENCHMARKING simple_update
                             user     system      total        real
Avram simple_update      0.073312   0.101071   0.174383 (  1.371658)
Crecto simple_update     0.044117   0.046060   0.090177 (  0.754066)
Granite simple_update    0.022183   0.036054   0.058237 (  0.755027)
Jennifer simple_update   0.027320   0.050211   0.077531 (  0.759229)
PgORM simple_update      0.008919   0.023123   0.032042 (  0.447199)
BENCHMARKING simple_delete
                             user     system      total        real
Avram simple_delete      0.033921   0.049466   0.083387 (  0.765764)
Crecto simple_delete     0.031095   0.050755   0.081850 (  0.718908)
Granite simple_delete    0.023333   0.045345   0.068678 (  0.693689)
Jennifer simple_delete   0.038370   0.089591   0.127961 (  1.255163)
PgORM simple_delete      0.008872   0.025366   0.034238 (  0.442181)
```

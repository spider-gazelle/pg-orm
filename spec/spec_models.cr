require "time"
require "uuid"

require "../src/pg-orm"

class Snowflake < PgORM::Base
  default_primary_key id : Int32?
  table "snowflakes"
  attribute shape : String = UUID.random.to_s
  attribute meltiness : Int32 = Random.rand(100).to_i
  attribute personality : String = UUID.random.to_s
  attribute taste : String = UUID.random.to_s
  attribute vibe : String = UUID.random.to_s
  attribute size : Int32 = 0

  ensure_unique :meltiness
  ensure_unique :shape, callback: :id
  ensure_unique :personality do |personality|
    personality.downcase
  end

  ensure_unique scope: [:taste, :vibe], field: :taste do |taste, vibe|
    {taste.downcase, vibe.downcase}
  end

  ensure_unique scope: [:vibe, :size], field: :vibe, callback: :dip

  def dip(vibe : String, size : Int32)
    {vibe.downcase, size}
  end

  def id(value : T) forall T
    value
  end
end

class Tree < PgORM::Base
  attribute id : Int64
  attribute roots : Array(String) = -> { [] of String }

  def self.by_root_id(id)
    Tree.find_all_by_sql(<<-SQL)
      SELECT * from #{table_name} WHERE '#{id}' = ANY(roots)
    SQL
  end

  def self.by_root_id_where(id)
    Tree.where("$1 = ANY(roots)", id)
  end
end

class Root < PgORM::Base
  attribute id : Int64
  attribute length : Float64
end

# Timestamps

class Timo < PgORM::Base
  include PgORM::Timestamps

  attribute id : Int64
  attribute name : String
end

class Group < PgORM::Base
  table :groups

  attribute id : Int32
  attribute name : String
  attribute description : String?
end

class User < PgORM::Base
  include PgORM::Timestamps
  table :users

  primary_key :uuid

  attribute uuid : UUID
  attribute group_id : Int32
  attribute name : String
end

class Author < PgORM::Base
  table :authors

  attribute id : Int32
  attribute name : String
  has_many :books
end

class Book < PgORM::Base
  table :books

  attribute id : Int32
  attribute author_id : Int32
  attribute name : String

  belongs_to :author
end

class Supplier < PgORM::Base
  table :suppliers

  attribute id : Int32
  has_one :account
end

class Account < PgORM::Base
  table :accounts

  attribute id : Int32
  attribute supplier_id : Int32?
  belongs_to :supplier
end

class AuthorAutosave < PgORM::Base
  table "authors"

  attribute id : Int32
  attribute name : String

  has_many :books, autosave: true, foreign_key: "author_id", serialize: true
end

class AuthorNoAutosave < PgORM::Base
  table "authors"

  attribute id : Int32
  attribute name : String

  has_many :books, autosave: false, foreign_key: "author_id"
end

class BookAutosave < PgORM::Base
  table "books"

  attribute id : Int32
  attribute author_id : Int32
  attribute name : String

  belongs_to :author, autosave: true
end

class BookNoAutosave < PgORM::Base
  table "books"

  attribute id : Int32
  attribute author_id : Int32
  attribute name : String

  belongs_to :author, autosave: false
end

class SupplierAutosave < PgORM::Base
  table "suppliers"
  attribute id : Int32
  has_one :account, autosave: true, foreign_key: "supplier_id"
end

class SupplierNoAutosave < PgORM::Base
  table "suppliers"
  attribute id : Int32
  has_one :account, autosave: false, foreign_key: "supplier_id"
end

class SupplierDependentNullify < PgORM::Base
  table "suppliers"
  attribute id : Int32
  has_one :account, dependent: :nullify, foreign_key: "supplier_id"
end

class SupplierDependentDelete < PgORM::Base
  table "suppliers"
  attribute id : Int32
  has_one :account, dependent: :delete, foreign_key: "supplier_id"
end

class SupplierDependentDestroy < PgORM::Base
  table "suppliers"
  attribute id : Int32
  has_one :account, dependent: :destroy, foreign_key: "supplier_id"
end

class AccountDependentDelete < PgORM::Base
  table "accounts"

  attribute id : Int32
  attribute supplier_id : Int32?

  belongs_to :supplier, dependent: :delete
end

class AccountDependentDestroy < PgORM::Base
  table "accounts"

  attribute id : Int32
  attribute supplier_id : Int32?

  belongs_to :supplier, dependent: :destroy
end

class BasicModel < PgORM::Base
  table "models"

  attribute id : Int64
  attribute name : String
  attribute address : String?
  attribute age : Int32 = 0
  attribute hash : Hash(String, String) = {} of String => String
end

class ModelWithDefaults < PgORM::Base
  default_primary_key id : Int64
  table "models"
  attribute name : String = "bob"
  attribute address : String?
  attribute age : Int32 = 23
end

class ModelWithCallbacks < PgORM::Base
  table "models"

  attribute id : Int64
  attribute name : String
  attribute address : String?
  attribute age : Int32 = 10

  before_create :update_name
  before_save :set_address
  before_update :set_age

  before_destroy do
    self.name = "joe"
  end

  def update_name
    self.name = "bob"
  end

  def set_address
    self.address = "23"
  end

  def set_age
    self.age = 30
  end
end

class ModelWithValidations < PgORM::Base
  table "models"

  attribute id : Int64
  attribute name : String
  attribute address : String?
  attribute age : Int32 = 10

  validates :name, presence: true
  validates :age, numericality: {greater_than: 20}
end

class LittleBitPersistent < PgORM::Base
  table "models"

  attribute id : Int64
  attribute name : String
  attribute address : String?, persistence: false
  attribute age : Int32
end

class ConvertedFields < PgORM::Base
  table "converter"

  attribute id : Int64
  attribute name : String
  attribute time : Time, converter: Time::EpochConverter
end

class EnumFields < PgORM::Base
  table :enums

  enum Status
    Opened
    Closed
    Duplicated
  end

  enum Role
    Issue    = 1
    Bug      = 2
    Critical = 3
  end

  @[Flags]
  enum Permissions : Int64
    Read
    Write
  end

  attribute id : Int64
  attribute status : Status?
  attribute role : Role = Role::Issue
  attribute permissions : Permissions = Permissions::Read | Permissions::Write
  attribute active : Bool = false
end

class ModelWithComputedFields < PgORM::Base
  table :compute

  attribute id : Int64
  attribute name : String
  attribute ts : Int64
  attribute description : String, read_only: true
end

class Arrtest < PgORM::Base
  attribute id : Int64
  attribute arr1 : Array(String)? = nil
  attribute arr2 : Array(String) = [] of String
end

class CompositeKeys < PgORM::Base
  primary_key :key_one, :key_two

  attribute key_one : String
  attribute key_two : String
  attribute payload : String
end

class Article < PgORM::Base
  table :articles

  attribute id : Int64
  attribute title : String
  attribute content : String?
  attribute published : Bool = false
end

require "db"
require "json"
require "yaml"
require "uri"
require "./settings"
require "./pg-adapter"
require "./changefeed"
require "./errors"

module PgORM::Database
  extend Settings

  @@pool : DB::Database?
  @@connections = {} of LibC::ULong => DB::Connection
  @@transactions = {} of LibC::ULong => DB::Transaction
  @@info : Info?

  def self.configure : Nil
    Settings.configure do |settings|
      yield settings
    end
    enable_cdc
  end

  # Parse a postgres connection string URL. This may come from an environment variable.
  def self.parse(uri : String | URI) : Nil
    Settings.parse(uri)
    enable_cdc
  end

  def self.adapter(builder : Query::Builder) : PostgreSQL
    PostgreSQL.new(builder)
  end

  def self.quote(name : Symbol | String, io : IO)
    PostgreSQL.quote(name, io)
  end

  def self.info : Info
    @@info ||= Info.new
  end

  def self.pool : DB::Database
    @@pool ||= DB.open(Settings.to_uri)
  end

  def self.checkout : DB::Connection
    @@connections[object_id] ||= pool.checkout
  end

  def self.release : Nil
    @@connections.delete(object_id).try(&.release)
    if tx = @@transactions.delete(object_id)
      tx.rollback unless tx.closed?
    end
  end

  def self.with_connection
    if db = @@connections[object_id]?
      yield db
    else
      begin
        yield checkout
      ensure
        release
      end
    end
  end

  def self.connection
    if db = @@connections[object_id]?
      yield db
    else
      pool.using_connection { |db_| yield db_ }
    end
  end

  def self.begin_transaction : DB::Transaction
    @@transactions[object_id] ||= checkout.begin_transaction
  end

  def self.transaction
    if tx = @@transactions[Fiber.current.object_id]?
      transaction(tx.begin_transaction) { |tx_| yield tx_ }
    else
      with_connection do |conn|
        transaction(conn.begin_transaction) { |tx_| yield tx_ }
      end
    end
  end

  private def self.transaction(tx : DB::Transaction)
    id = Fiber.current.object_id
    @@transactions[id] = tx

    begin
      yield tx
    rescue ex
      tx.rollback unless tx.closed?
      raise ex unless ex.is_a?(DB::Rollback)
    else
      tx.commit unless tx.closed?
    ensure
      case tx
      when DB::TopLevelTransaction
        @@transactions.delete(id)
      when DB::SavePointTransaction
        @@transactions[id] = tx.@parent
      else
        raise "unsupported transaction type: #{tx.class.name}"
      end
    end
  end

  @@cdc : ChangeFeedHandler?
  at_exit { @@cdc.try &.stop }

  # :nodoc:
  def self.listen_change_feed(table : String, receiver : ChangeReceiver)
    @@cdc.try &.add_listener(table, receiver)
  end

  # :nodoc:
  def self.stop_change_feed(table : String)
    @@cdc.try &.remove_listener(table)
  end

  private def self.enable_cdc : Nil
    return if @@cdc
    @@cdc = ChangeFeedHandler.new(Settings.to_uri)
    @@cdc.not_nil!.start
  end

  # :nodoc:
  @[AlwaysInline]
  private def self.object_id : UInt64
    Fiber.current.object_id
  end

  struct Info
    include JSON::Serializable
    include YAML::Serializable
    getter table_infos : Array(TableInfo)

    protected def initialize
      @table_infos =
        (Database.connection &.query(SQL_QUERY) { |rs| ColumnInfo.from_rs(rs) })
          .group_by(&.table)
          .tap { |g| g.each do |t, cs|
            cs.each { |c| t.columns << c }
          end }
          .keys
    end

    def table?(name : String)
      table_names.includes?(name)
    end

    def table_names
      table_infos.map(&.table_name.downcase)
    end

    def table(name : String) : TableInfo?
      table_infos.find(&.table_name.==(name))
    end

    # :nodoc:
    SQL_QUERY = <<-SQL
      SELECT columns.table_name,
            tables.table_type,
            columns.table_schema,
            columns.table_catalog,
            columns.column_name,
            columns.is_nullable,
            columns.column_default,
            columns.data_type
      FROM information_schema.columns as columns
      JOIN information_schema.tables as tables
        ON tables.table_name = columns.table_name
        AND tables.table_catalog = columns.table_catalog
        AND tables.table_schema = columns.table_schema
      WHERE columns.table_schema='public';
    SQL
  end

  struct TableInfo
    include JSON::Serializable
    include YAML::Serializable

    getter table_name : String
    getter table_type : String
    getter table_schema : String
    getter table_catalog : String
    getter columns = [] of ColumnInfo

    def initialize(
      @table_name,
      @table_type,
      @table_schema,
      @table_catalog
    )
    end

    def table?
      table_type == "BASE TABLE"
    end

    def column?(name : String)
      column_names.includes?(name)
    end

    def column_names
      columns.map(&.column_name)
    end

    def column(name : String) : ColumnInfo?
      columns.find(&.column_name.==(name))
    end

    def to_s(io : IO) : Nil
      io << (table_type.split)[-1] << " " << table_schema << "." << table_name
    end
  end

  struct ColumnInfo
    include DB::Serializable
    include JSON::Serializable
    include YAML::Serializable

    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    property table_catalog : String
    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    property table_schema : String
    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    property table_name : String
    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    property table_type : String
    property column_name : String
    property is_nullable : String
    property column_default : String?
    property data_type : String

    def nilable?
      is_nullable == "YES"
    end

    def table
      TableInfo.new(
        table_name,
        table_type,
        table_schema,
        table_catalog
      )
    end
  end
end

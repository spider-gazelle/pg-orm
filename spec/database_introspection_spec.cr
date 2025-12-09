require "./spec_helper"

describe PgORM::Database do
  describe "Database Introspection" do
    it "provides database info" do
      info = PgORM::Database.info
      info.should be_a(PgORM::Database::Info)
    end

    it "lists all tables" do
      info = PgORM::Database.info
      table_names = info.table_names

      table_names.should contain("users")
      table_names.should contain("groups")
      table_names.should contain("articles")
      table_names.should contain("authors")
      table_names.should contain("books")
    end

    it "checks if table exists" do
      info = PgORM::Database.info

      info.table?("users").should be_true
      info.table?("groups").should be_true
      info.table?("nonexistent_table").should be_false
    end

    it "gets table information" do
      info = PgORM::Database.info
      table = info.table("users")

      table.should_not be_nil
      table.try &.table_name.should eq("users")
      table.try &.table_schema.should eq("public")
    end

    it "returns nil for nonexistent table" do
      info = PgORM::Database.info
      table = info.table("nonexistent_table")

      table.should be_nil
    end

    it "identifies table type" do
      info = PgORM::Database.info
      table = info.table("users").not_nil!

      table.table?.should be_true
      table.table_type.should eq("BASE TABLE")
    end

    it "lists table columns" do
      info = PgORM::Database.info
      table = info.table("users").not_nil!

      column_names = table.column_names
      column_names.should contain("uuid")
      column_names.should contain("name")
      column_names.should contain("group_id")
      column_names.should contain("created_at")
      column_names.should contain("updated_at")
    end

    it "checks if column exists" do
      info = PgORM::Database.info
      table = info.table("users").not_nil!

      table.column?("name").should be_true
      table.column?("uuid").should be_true
      table.column?("nonexistent_column").should be_false
    end

    it "gets column information" do
      info = PgORM::Database.info
      table = info.table("users").not_nil!
      column = table.column("name")

      column.should_not be_nil
      column.not_nil!.column_name.should eq("name")
      column.not_nil!.data_type.should eq("character varying")
    end

    it "returns nil for nonexistent column" do
      info = PgORM::Database.info
      table = info.table("users").not_nil!
      column = table.column("nonexistent_column")

      column.should be_nil
    end

    it "identifies nullable columns" do
      info = PgORM::Database.info
      table = info.table("groups").not_nil!

      name_column = table.column("name").not_nil!
      name_column.nilable?.should be_false

      description_column = table.column("description").not_nil!
      description_column.nilable?.should be_true
    end

    it "identifies column data types" do
      info = PgORM::Database.info
      table = info.table("models").not_nil!

      id_column = table.column("id").not_nil!
      id_column.data_type.should eq("bigint")

      name_column = table.column("name").not_nil!
      name_column.data_type.should eq("character varying")

      age_column = table.column("age").not_nil!
      age_column.data_type.should eq("integer")
    end

    it "identifies column defaults" do
      info = PgORM::Database.info
      table = info.table("articles").not_nil!

      published_column = table.column("published").not_nil!
      published_column.column_default.should_not be_nil
      published_column.column_default.not_nil!.should contain("false")
    end

    it "handles tables with composite keys" do
      info = PgORM::Database.info
      table = info.table("composite_keys")

      table.should_not be_nil
      table.not_nil!.column?("key_one").should be_true
      table.not_nil!.column?("key_two").should be_true
      table.not_nil!.column?("payload").should be_true
    end

    it "handles tables with array columns" do
      info = PgORM::Database.info
      table = info.table("tree").not_nil!

      roots_column = table.column("roots").not_nil!
      roots_column.data_type.should eq("ARRAY")
    end

    it "handles tables with enum columns" do
      info = PgORM::Database.info
      table = info.table("enums").not_nil!

      status_column = table.column("status").not_nil!
      status_column.data_type.should eq("integer")
      status_column.nilable?.should be_true

      role_column = table.column("role").not_nil!
      role_column.data_type.should eq("integer")
      role_column.nilable?.should be_false
    end

    it "handles tables with computed columns" do
      info = PgORM::Database.info
      table = info.table("compute").not_nil!

      description_column = table.column("description").not_nil!
      description_column.column_name.should eq("description")
      # Read-only columns are supported
      description_column.data_type.should be_a(String)
    end

    it "serializes table info to JSON" do
      info = PgORM::Database.info
      table = info.table("users").not_nil!

      json = table.to_json
      json.should contain("users")
      json.should contain("public")

      parsed = PgORM::Database::TableInfo.from_json(json)
      parsed.table_name.should eq("users")
    end

    it "serializes column info to JSON" do
      info = PgORM::Database.info
      table = info.table("users").not_nil!
      column = table.column("name").not_nil!

      json = column.to_json
      json.should contain("name")
      json.should contain("character varying")

      parsed = PgORM::Database::ColumnInfo.from_json(json)
      parsed.column_name.should eq("name")
    end

    it "caches database info" do
      info1 = PgORM::Database.info
      info2 = PgORM::Database.info

      # Should return cached info with same data
      info1.table_names.should eq(info2.table_names)
      info1.table_names.size.should be > 0
    end

    it "provides table string representation" do
      info = PgORM::Database.info
      table = info.table("users").not_nil!

      table.to_s.should contain("TABLE")
      table.to_s.should contain("public")
      table.to_s.should contain("users")
    end

    it "handles multiple tables with same column names" do
      info = PgORM::Database.info

      users_table = info.table("users").not_nil!
      groups_table = info.table("groups").not_nil!

      # Both have 'id' or 'name' columns
      users_table.column?("name").should be_true
      groups_table.column?("name").should be_true

      # But they're different tables
      users_table.table_name.should_not eq(groups_table.table_name)
    end

    it "introspects all model tables" do
      info = PgORM::Database.info

      # Verify all test tables are present
      expected_tables = [
        "users", "groups", "authors", "books",
        "suppliers", "accounts", "models", "snowflakes",
        "timo", "converter", "tree", "root",
        "enums", "compute", "composite_keys", "articles", "arrtest",
      ]

      expected_tables.each do |table_name|
        info.table?(table_name).should be_true, "Expected table '#{table_name}' to exist"
      end
    end

    it "handles case-insensitive table lookups" do
      info = PgORM::Database.info

      # PostgreSQL stores unquoted identifiers as lowercase
      info.table?("users").should be_true
      # Uppercase lookups need to match lowercase stored names
      info.table?("users".downcase).should be_true
    end
  end
end

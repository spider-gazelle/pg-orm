require "spec"
require "../src/pg-orm"
require "./spec_models"

alias SpecConnection = PgORM::Database

module Helper
  @@group : Group? = nil

  def self.group_id
    @@group.not_nil!.id
  end

  def self.set(group)
    @@group ||= group
  end
end

def group_id
  Helper.group_id
end

Spec.before_suite do
  SpecConnection.parse(ENV["PG_DATABASE_URL"])

  SpecConnection.connection do |db|
    db.exec "DROP TABLE IF EXISTS groups;"
    db.exec "DROP TABLE IF EXISTS users;"
    db.exec "DROP TABLE IF EXISTS authors;"
    db.exec "DROP TABLE IF EXISTS books;"
    db.exec "DROP TABLE IF EXISTS suppliers;"
    db.exec "DROP TABLE IF EXISTS accounts;"
    db.exec "DROP TABLE IF EXISTS models"
    db.exec "DROP TABLE IF EXISTS snowflakes"
    db.exec "DROP TABLE IF EXISTS timo"
    db.exec "DROP TABLE IF EXISTS converter"
    db.exec "DROP TABLE IF EXISTS tree"
    db.exec "DROP TABLE IF EXISTS root"
    db.exec "DROP TABLE IF EXISTS enums"
    db.exec "DROP TABLE IF EXISTS compute"
    db.exec "DROP TABLE IF EXISTS composite_keys"
    db.exec <<-SQL
    CREATE TABLE groups (
      id SERIAL NOT NULL PRIMARY KEY,
      name VARCHAR NOT NULL,
      description TEXT
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE users (
      uuid UUID NOT NULL PRIMARY KEY,
      group_id INT NOT NULL,
      name VARCHAR NOT NULL,
      created_at TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE authors (
      id SERIAL NOT NULL PRIMARY KEY,
      name VARCHAR NOT NULL
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE books (
      id SERIAL NOT NULL PRIMARY KEY,
      author_id INT NOT NULL,
      name VARCHAR NOT NULL
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE suppliers (
      id SERIAL NOT NULL PRIMARY KEY
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE accounts (
      id SERIAL NOT NULL PRIMARY KEY,
      supplier_id INT
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE models (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      name VARCHAR NOT NULL,
      address TEXT NULL,
      age INT NOT NULL,
      hash jsonb NULL
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE snowflakes (
      id SERIAL NOT NULL PRIMARY KEY,
      shape TEXT NOT NULL,
      meltiness INT NOT NULL,
      personality TEXT NOT NULL,
      taste TEXT NOT NULL,
      vibe TEXT NOT NULL,
      size INT NOT NULL
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE timo (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      name VARCHAR NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE converter (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      name VARCHAR NOT NULL,
      time TIMESTAMPTZ NOT NULL
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE tree (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      roots TEXT [] NOT NULL
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE root (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      length numeric NOT NULL
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE enums (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      status int,
      role int NOT NULL,
      permissions int NOT NULL,
      active boolean NOT NULL
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE compute (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      name VARCHAR NOT NULL,
      ts bigint,
      description TEXT GENERATED ALWAYS AS (name || ' description') STORED,
      starting_time TIME GENERATED ALWAYS AS ((TO_TIMESTAMP(ts::BIGINT) AT TIME ZONE 'UTC')::TIME) STORED
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE composite_keys (
      key_one TEXT NOT NULL,
      key_two TEXT NOT NULL,
      payload TEXT NOT NULL,
      PRIMARY KEY (key_one, key_two)
    );
    SQL

    db.exec <<-SQL
    CREATE TABLE IF NOT EXISTS arrtest (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      arr1 TEXT [],
      arr2 TEXT[] NOT NULL DEFAULT '{}'
    );
    SQL

    db.exec <<-SQL
      INSERT INTO arrtest(arr2) values('{"three","four"}');
    SQL
  end

  1.upto(2) do |_|
    group = Group.create(name: "Group X")
    Helper.set(group)
    1.upto(2) do |i|
      User.create(uuid: UUID.random, group_id: group.id, name: "User X-#{i}")
    end
  end
end

Spec.after_suite do
  SpecConnection.connection do |db|
    db.exec "DROP TABLE IF EXISTS groups;"
    db.exec "DROP TABLE IF EXISTS users;"
    db.exec "DROP TABLE IF EXISTS authors;"
    db.exec "DROP TABLE IF EXISTS books;"
    db.exec "DROP TABLE IF EXISTS suppliers;"
    db.exec "DROP TABLE IF EXISTS accounts;"
    db.exec "DROP TABLE IF EXISTS models"
    db.exec "DROP TABLE IF EXISTS snowflakes"
    db.exec "DROP TABLE IF EXISTS timo"
    db.exec "DROP TABLE IF EXISTS converter"
    db.exec "DROP TABLE IF EXISTS tree"
    db.exec "DROP TABLE IF EXISTS root"
    db.exec "DROP TABLE IF EXISTS enums"
    db.exec "DROP TABLE IF EXISTS compute"
    db.exec "DROP TABLE IF EXISTS arrtest"
    db.exec "DROP TABLE IF EXISTS composite_keys"
  end
end

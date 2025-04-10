require "./spec_helper"
require "uuid"

class Foo < PgORM::Base
  table :foo
  attribute id : Int32
end

class Bar < PgORM::Base
  primary_key :uuid

  attribute uuid : UUID
end

class Baz < PgORM::Base
  table :baz_table
  attribute id : Int32
  attribute name : String
  attribute about : String? = nil
end

describe PgORM::Table do
  it "test table name" do
    Foo.table_name.should eq("foo")
    Bar.table_name.should eq("bar")
    Baz.table_name.should eq("baz_table")
  end

  it "test primary key" do
    :id.should eq(Foo.primary_key)
    :uuid.should eq(Bar.primary_key)
    :id.should eq(Baz.primary_key)
  end

  it "test id" do
    bar = Bar.new(uuid: UUID.random)
    bar.uuid.should eq(bar.id)
    expect_raises(Exception) do
      Bar.new.id
    end
  end

  it "test id?" do
    bar = Bar.new(uuid: UUID.random)
    bar.uuid.should eq(bar.id?)
    Bar.new.id?.should eq(nil)
  end

  it "test initializers" do
    baz = Baz.new
    baz.id?.should eq nil
    baz.name?.should eq nil
    baz.about.should eq nil

    baz = Baz.new(id: 1)
    baz.id?.should eq 1
    baz.name?.should eq nil
    baz.about.should eq nil

    baz = Baz.new(id: 1, about: "description")
    baz.id?.should eq 1
    baz.name?.should eq nil
    baz.about.should eq("description")
  end

  it "test primary key types" do
    Foo::PrimaryKeyType.should eq(Int32)
    Bar::PrimaryKeyType.should eq(UUID)
    Baz::PrimaryKeyType.should eq(Int32)
  end

  it "test from json" do
    foo = Foo.from_json(%({"id":12345}))
    foo.id.should eq(12345)

    bar = Bar.from_json(%({"uuid":"b7e7cdbc-16c8-43fb-aab8-6cf3c0ff10f6"}))
    bar.uuid.should eq(UUID.new("b7e7cdbc-16c8-43fb-aab8-6cf3c0ff10f6"))

    baz = Baz.from_json(%({"id": 12}))
    baz.id.should eq(12)
    baz.name?.should eq nil
    baz.about.should eq nil
  end

  it "test to json" do
    baz = Baz.new(name: "B", about: "C", id: 2)
    baz.to_json.should eq(%({"id":2,"name":"B","about":"C"}))
  end

  it "should work with models without primary key" do
    model = ModelWithDefaults.new

    model.primary_key.should eq(:id)
    ModelWithDefaults::PrimaryKeyType.should eq(typeof(model.id))
  end
end

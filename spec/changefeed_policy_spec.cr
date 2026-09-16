require "./spec_helper"

class TelemetryModel < PgORM::Base
  table "changefeed_models"
  attribute id : Int64
  attribute name : String
  attribute last_seen : Int64 = 0
  attribute playlist_item_id : String?
  changefeed_ignore_updates :last_seen, :playlist_item_id
end

class OverrideTelemetryModel < PgORM::Base
  table "changefeed_models"
  attribute id : Int64
  attribute name : String
  attribute last_seen : Int64 = 0
  attribute playlist_item_id : String?
  changefeed_ignore_updates :last_seen
end

class DefaultTelemetryModel < PgORM::Base
  table "changefeed_models"
  attribute id : Int64
  attribute name : String
  attribute last_seen : Int64 = 0
  attribute playlist_item_id : String?
end

class AllUpdatesTelemetryModel < PgORM::Base
  table "changefeed_models"
  attribute id : Int64
  attribute name : String
  attribute last_seen : Int64 = 0
  attribute playlist_item_id : String?
  changefeed_ignore_updates
end

private def receive_policy_event(channel)
  select
  when event = channel.receive
    event
  when timeout(2.seconds)
    raise "Timed out waiting for changefeed event"
  end
end

describe "model changefeed update policies" do
  before_each do
    SpecConnection.exec_sql("DROP TABLE IF EXISTS changefeed_models")
    SpecConnection.exec_sql(<<-SQL)
      CREATE TABLE changefeed_models (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        name text NOT NULL,
        last_seen bigint NOT NULL,
        playlist_item_id text
      )
      SQL
    sleep 0.1.seconds
  end

  after_each do
    SpecConnection.exec_sql("DROP TABLE IF EXISTS changefeed_models")
  end

  it "isolates model policies and returns a fresh array" do
    TelemetryModel.changefeed_ignored_update_columns.should eq(["last_seen", "playlist_item_id"])
    OverrideTelemetryModel.changefeed_ignored_update_columns.should eq(["last_seen"])
    DefaultTelemetryModel.changefeed_ignored_update_columns.should be_nil
    AllUpdatesTelemetryModel.changefeed_ignored_update_columns.should eq([] of String)
    TelemetryModel.changefeed_ignored_update_columns.not_nil!.clear
    TelemetryModel.changefeed_ignored_update_columns.should eq(["last_seen", "playlist_item_id"])
  end

  it "persists ignored updates without events while notifying mixed updates, creates, and deletes" do
    events = Channel(Tuple(PgORM::ChangeReceiver::Event, TelemetryModel)).new(10)
    feed = TelemetryModel.changes
    spawn { feed.on { |change| events.send({change.event, change.value}) } }
    Fiber.yield

    model = TelemetryModel.create!(name: "display")
    receive_policy_event(events)[0].created?.should be_true
    model.last_seen = 10
    model.save!
    model.playlist_item_id = "item-1"
    model.save!
    model.reload!
    model.last_seen.should eq(10)
    model.playlist_item_id.should eq("item-1")
    select
    when events.receive
      fail "Telemetry-only changes must not notify"
    when timeout(0.2.seconds)
    end

    model.name = "updated"
    model.last_seen = 20
    model.save!
    change = receive_policy_event(events)
    change[0].updated?.should be_true
    change[1].name.should eq("updated")
    change[1].last_seen.should eq(20)
    change[1].last_seen_changed?.should be_true

    model.destroy
    receive_policy_event(events)[0].deleted?.should be_true
  ensure
    feed.try &.stop
  end

  it "preserves an installed policy when a model has no declaration" do
    TelemetryModel.changes.stop
    events = Channel(PgORM::ChangeReceiver::Event).new(10)
    feed = DefaultTelemetryModel.changes
    spawn { feed.on { |change| events.send(change.event) } }
    Fiber.yield
    model = DefaultTelemetryModel.create!(name: "default")
    receive_policy_event(events).created?.should be_true
    model.last_seen = 1
    model.save!
    select
    when events.receive
      fail "Default subscriptions must preserve the installed policy"
    when timeout(0.2.seconds)
    end
    model.name = "changed"
    model.save!
    receive_policy_event(events).updated?.should be_true
  ensure
    feed.try &.stop
  end

  it "notifies all changed columns without a configured policy" do
    events = Channel(PgORM::ChangeReceiver::Event).new(10)
    feed = DefaultTelemetryModel.changes
    spawn { feed.on { |change| events.send(change.event) } }
    Fiber.yield
    model = DefaultTelemetryModel.create!(name: "default")
    receive_policy_event(events).created?.should be_true
    model.last_seen = 1
    model.save!
    receive_policy_event(events).updated?.should be_true
  ensure
    feed.try &.stop
  end

  it "allows an empty override to notify every changed column" do
    events = Channel(PgORM::ChangeReceiver::Event).new(10)
    feed = AllUpdatesTelemetryModel.changes
    spawn { feed.on { |change| events.send(change.event) } }
    Fiber.yield
    model = AllUpdatesTelemetryModel.create!(name: "all")
    receive_policy_event(events).created?.should be_true
    model.last_seen = 1
    model.save!
    receive_policy_event(events).updated?.should be_true
  ensure
    feed.try &.stop
  end

  it "fails on conflicting policies and can retry after the conflict is resolved" do
    original_events = Channel(PgORM::ChangeReceiver::Event).new(10)
    original_feed = TelemetryModel.changes
    spawn { original_feed.on { |change| original_events.send(change.event) } }
    Fiber.yield
    expect_raises(ArgumentError, "Conflicting CDC update policy") { OverrideTelemetryModel.changes }
    # A second attempt must validate again, rather than bypass registration
    # because the first failed subscription leaked a model callback.
    expect_raises(ArgumentError, "Conflicting CDC update policy") { OverrideTelemetryModel.changes }
    TelemetryModel.create!(name: "original listener remains active")
    receive_policy_event(original_events).created?.should be_true
    original_feed.stop
    bus = EventBus.new(ENV["PG_DATABASE_URL"])
    bus.replace_cdc_update_policy("changefeed_models", ["last_seen"], ["last_seen", "playlist_item_id"]).should be_true
    events = Channel(PgORM::ChangeReceiver::Event).new(10)
    feed = OverrideTelemetryModel.changes
    spawn { feed.on { |change| events.send(change.event) } }
    Fiber.yield
    model = OverrideTelemetryModel.create!(name: "retry")
    receive_policy_event(events).created?.should be_true
    model.playlist_item_id = "notify"
    model.save!
    receive_policy_event(events).updated?.should be_true
  ensure
    feed.try &.stop
    original_feed.try &.stop
    bus.try &.close
  end
end

describe "changefeed declaration validation" do
  {
    ":missing"                              => "changefeed_ignore_updates: missing is not a persisted attribute of InvalidPolicy",
    ":temporary"                            => "changefeed_ignore_updates: temporary is not a persisted attribute of InvalidPolicy",
    ":id"                                   => "changefeed_ignore_updates: id cannot be ignored",
    "\"name\""                              => "changefeed_ignore_updates expects attribute symbols",
    "database_columns: [:id]"               => "changefeed_ignore_updates: id cannot be ignored",
    "database_columns: :search_vector"      => "changefeed_ignore_updates database_columns expects an array of symbols",
    "database_columns: [\"search_vector\"]" => "changefeed_ignore_updates database_columns expects an array of symbols",
  }.each do |attribute, message|
    it "rejects invalid declaration #{attribute} at compile time" do
      source = <<-CRYSTAL
        require "../src/pg-orm"
        class InvalidPolicy < PgORM::Base
          attribute id : Int64
          attribute temporary : String?, persistence: false
          changefeed_ignore_updates #{attribute}
        end
        CRYSTAL
      output = IO::Memory.new
      File.tempfile("changefeed_policy", ".fixture", dir: "spec") do |file|
        begin
          file << source
          file.flush
          status = Process.run("crystal", ["build", "--no-codegen", "--error-trace", file.path], output: output, error: output)
          status.success?.should be_false
        ensure
          File.delete(file.path)
        end
      end
      output.to_s.should contain(message)
    end
  end
end

describe "inherited changefeed declarations" do
  it "inherits and overrides abstract policies without registry name collisions" do
    source = <<-CRYSTAL
      require "../src/pg-orm"
      module PolicyNamespace
        abstract class Record < PgORM::Base
          attribute id : Int64
          attribute name : String?
          attribute last_seen : Int64 = 0
          changefeed_ignore_updates :last_seen, database_columns: [:search_vector]
        end
      end
      class PolicyNamespace__Record < PolicyNamespace::Record
      end
      class OverridePolicy < PolicyNamespace::Record
        changefeed_ignore_updates :name
      end
      class EmptyPolicy < PolicyNamespace::Record
        changefeed_ignore_updates
      end
      class DatabaseOnlyPolicy < PolicyNamespace::Record
        changefeed_ignore_updates database_columns: [:other_vector]
      end
      raise "inherited policy changed" unless PolicyNamespace__Record.changefeed_ignored_update_columns == ["last_seen", "search_vector"]
      PolicyNamespace__Record.changefeed_ignored_update_columns.not_nil!.clear
      raise "inherited policy mutated" unless PolicyNamespace__Record.changefeed_ignored_update_columns == ["last_seen", "search_vector"]
      raise "override ignored" unless OverridePolicy.changefeed_ignored_update_columns == ["name"]
      raise "empty override ignored" unless EmptyPolicy.changefeed_ignored_update_columns == [] of String
      raise "database-only override ignored" unless DatabaseOnlyPolicy.changefeed_ignored_update_columns == ["other_vector"]
      CRYSTAL
    output = IO::Memory.new
    File.tempfile("changefeed_policy", ".fixture", dir: "spec") do |file|
      begin
        file << source
        file.flush
        status = Process.run("crystal", ["run", "--error-trace", file.path], output: output, error: output)
        status.success?.should be_true, output.to_s
      ensure
        File.delete(file.path)
      end
    end
  end
end

class GeneratedColumnModel < PgORM::Base
  table "generated_changefeed_models"
  attribute id : Int64
  attribute name : String
  attribute code : String
  changefeed_ignore_updates :name, database_columns: [:search_vector, :name, :search_vector]
end

class UnknownDatabaseColumnModel < PgORM::Base
  table "generated_changefeed_models"
  attribute id : Int64
  attribute name : String
  attribute code : String
  changefeed_ignore_updates database_columns: [:missing_column]
end

describe "database column changefeed exclusions" do
  before_each do
    SpecConnection.exec_sql("DROP TABLE IF EXISTS generated_changefeed_models")
    SpecConnection.exec_sql(<<-SQL)
      CREATE TABLE generated_changefeed_models (
        id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        name text NOT NULL,
        code text NOT NULL,
        search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple', name || ' ' || code)) STORED
      )
      SQL
    sleep 0.1.seconds
  end

  after_each do
    SpecConnection.exec_sql("DROP TABLE IF EXISTS generated_changefeed_models")
  end

  it "ignores generated columns while notifying other fields that also update them" do
    GeneratedColumnModel.changefeed_ignored_update_columns.should eq(["name", "search_vector"])
    events = Channel(PgORM::ChangeReceiver::Event).new(10)
    feed = GeneratedColumnModel.changes
    spawn { feed.on { |change| events.send(change.event) } }
    Fiber.yield
    model = GeneratedColumnModel.create!(name: "original", code: "unchanged")
    receive_policy_event(events).created?.should be_true
    model.name = "updated"
    model.save!
    model.reload!
    model.name.should eq("updated")
    SpecConnection.connection do |db|
      db.scalar("SELECT search_vector::text FROM generated_changefeed_models WHERE id = $1", model.id).as(String).should contain("updated")
    end
    select
    when events.receive
      fail "Ignored source and generated column changes must not notify"
    when timeout(0.2.seconds)
    end

    model.code = "different"
    model.save!
    receive_policy_event(events).updated?.should be_true
    model.name = "mixed"
    model.code = "both"
    model.save!
    receive_policy_event(events).updated?.should be_true
    model.destroy
    receive_policy_event(events).deleted?.should be_true
  ensure
    feed.try &.stop
  end

  it "rejects unknown database columns at registration and can retry after schema repair" do
    2.times do
      expect_raises(ArgumentError, "Invalid ignored update column") { UnknownDatabaseColumnModel.changes }
    end
    SpecConnection.exec_sql("ALTER TABLE generated_changefeed_models ADD COLUMN missing_column text")
    events = Channel(PgORM::ChangeReceiver::Event).new(10)
    feed = UnknownDatabaseColumnModel.changes
    spawn { feed.on { |change| events.send(change.event) } }
    Fiber.yield
    model = UnknownDatabaseColumnModel.create!(name: "retry", code: "new")
    receive_policy_event(events).created?.should be_true
    model.code = "changed"
    model.save!
    receive_policy_event(events).updated?.should be_true
  ensure
    feed.try &.stop
  end
end

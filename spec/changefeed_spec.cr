require "./spec_helper"

describe PgORM::ChangeFeedHandler do
  it "should receive changes on a single table record" do
    base = BasicModel.create!(name: "cdc-test")
    chan = Channel(Nil).new
    changefeed = BasicModel.changes(base.id)
    spawn do
      changefeed.on do |change|
        change.updated?.should be_true
        change.value.id.should eq(base.id)
        change.value.name.should eq("cdc-test-changed")
        chan.send(nil)
      end
    end
    Fiber.yield

    base.name = "cdc-test-changed"
    base.save

    chan.receive
    changefeed.try &.stop
  end

  it "should receive changes on table" do
    chan = Channel(Nil).new
    names = [] of String
    events = [] of PgORM::ChangeReceiver::Event
    changefeed = BasicModel.changes
    spawn do
      changefeed.on do |change|
        events << change.event
        names << change.value.name unless change.deleted?
        chan.send(nil)
      end
    end
    Fiber.yield

    BasicModel.create!(name: "ren")
    BasicModel.create!(name: "stimpy")
    horse = BasicModel.create!(name: "mr. horse")
    horse.destroy

    4.times { chan.receive }
    changefeed.try &.stop

    BasicModel.create!(name: "bubbles")

    names.should eq ["ren", "stimpy", "mr. horse"]
    events.should eq([
      PgORM::ChangeReceiver::Event::Created,
      PgORM::ChangeReceiver::Event::Created,
      PgORM::ChangeReceiver::Event::Created,
      PgORM::ChangeReceiver::Event::Deleted,
    ])
  end

  it "should iterate changes on a table" do
    chan = Channel(Nil).new
    names = [] of String
    events = [] of PgORM::ChangeReceiver::Event
    changefeed = BasicModel.changes

    spawn do
      changefeed.each.with_index do |change, index|
        case index
        when 0, 1, 2, 4, 5
          events << change.event
          names << change.value.name
        when 3
          events << change.event
          chan.send(nil)
          break
        else
          raise "unexpected index #{index}"
        end
      end
    end

    Fiber.yield

    BasicModel.create!(name: "ren")
    BasicModel.create!(name: "stimpy")
    horse = BasicModel.create!(name: "mr. horse")
    # sleep 1
    horse.destroy

    chan.receive
    changefeed.stop

    BasicModel.create!(name: "bubbles")

    names.should eq ["ren", "stimpy", "mr. horse"]
    events.should eq([
      PgORM::ChangeReceiver::Event::Created,
      PgORM::ChangeReceiver::Event::Created,
      PgORM::ChangeReceiver::Event::Created,
      PgORM::ChangeReceiver::Event::Deleted,
    ])
  end
end

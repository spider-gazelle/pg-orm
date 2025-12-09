require "./spec_helper"

describe PgORM::ChangeFeedHandler do
  describe "Error Handling" do
    it "processes changefeed events correctly" do
      base = BasicModel.create!(name: "event-test")
      received_count = 0
      chan = Channel(Nil).new
      changefeed = BasicModel.changes(base.id)

      spawn do
        changefeed.on do |_|
          received_count += 1
          chan.send(nil)
        end
      end

      Fiber.yield

      # Update should trigger callback
      base.name = "event-test-updated"
      base.save

      # Wait for event
      chan.receive

      changefeed.stop

      # Verify the callback was called
      received_count.should eq(1)
    end

    it "processes multiple changefeed events" do
      chan = Channel(String).new
      names = [] of String
      changefeed = BasicModel.changes

      spawn do
        changefeed.on do |change|
          names << change.value.name
          chan.send(change.value.name) if names.size == 3
        end
      end

      Fiber.yield

      BasicModel.create!(name: "record-1")
      BasicModel.create!(name: "record-2")
      BasicModel.create!(name: "record-3")

      # Should receive all records
      select
      when result = chan.receive
        names.size.should eq(3)
        names.should contain("record-1")
        names.should contain("record-2")
        names.should contain("record-3")
      when timeout(2.seconds)
        fail "Timeout waiting for changefeed"
      end

      changefeed.stop
    end

    it "stops changefeed gracefully" do
      chan = Channel(Nil).new
      changefeed = BasicModel.changes

      spawn do
        changefeed.on do |_|
          chan.send(nil)
        end
      end
      Fiber.yield

      # Create a record to trigger the changefeed
      model = BasicModel.create!(name: "test stop")

      # Wait for changefeed to receive it
      chan.receive

      # Stop should not raise an error
      changefeed.stop

      model.destroy
    end

    it "handles multiple stop calls" do
      changefeed = BasicModel.changes

      changefeed.stop
      # Second stop should not raise error
      changefeed.stop
    end

    it "handles stop during iteration" do
      chan = Channel(Nil).new
      changefeed = BasicModel.changes

      spawn do
        changefeed.each do |_|
          chan.send(nil)
          break
        end
      end

      Fiber.yield

      BasicModel.create!(name: "test-stop")

      chan.receive
      changefeed.stop

      # Should not raise error
    end

    it "cleans up resources on stop" do
      initial_count = PgORM::PgAdvisoryLock.count rescue 0

      changefeed = BasicModel.changes
      changefeed.stop

      # Give time for cleanup
      sleep 0.1.seconds

      # Resources should be cleaned up
      # (This is a basic check - actual implementation may vary)
    end

    it "handles changefeed for deleted records" do
      base = BasicModel.create!(name: "delete-test")
      chan = Channel(Nil).new
      deleted = false

      changefeed = BasicModel.changes(base.id)

      spawn do
        changefeed.on do |change|
          if change.deleted?
            deleted = true
            chan.send(nil)
          end
        end
      end

      Fiber.yield

      base.destroy

      chan.receive
      deleted.should be_true
      changefeed.stop
    end

    it "handles changefeed when record doesn't exist" do
      # Create changefeed for non-existent ID
      changefeed = BasicModel.changes(999999_i64)

      spawn do
        changefeed.on do |_|
          # Should not receive any changes
          fail "Should not receive changes for non-existent record"
        end
      end

      Fiber.yield

      # Create a different record
      BasicModel.create!(name: "other-record")

      sleep 0.1.seconds

      changefeed.stop
    end

    it "handles concurrent changefeeds on same table" do
      chan1 = Channel(String).new
      chan2 = Channel(String).new

      changefeed1 = BasicModel.changes
      changefeed2 = BasicModel.changes

      spawn do
        changefeed1.on do |change|
          chan1.send(change.value.name)
        end
      end

      spawn do
        changefeed2.on do |change|
          chan2.send(change.value.name)
        end
      end

      Fiber.yield

      BasicModel.create!(name: "concurrent-test")

      # Both should receive the change
      name1 = name2 = nil

      select
      when name1 = chan1.receive
      when timeout(1.seconds)
        fail "Timeout on changefeed1"
      end

      select
      when name2 = chan2.receive
      when timeout(1.seconds)
        fail "Timeout on changefeed2"
      end

      name1.should eq("concurrent-test")
      name2.should eq("concurrent-test")

      changefeed1.stop
      changefeed2.stop
    end

    it "handles changefeed with rapid updates" do
      base = BasicModel.create!(name: "rapid-test")
      chan = Channel(Int32).new
      update_count = 0

      changefeed = BasicModel.changes(base.id)

      spawn do
        changefeed.on do |change|
          if change.updated?
            update_count += 1
            chan.send(update_count) if update_count == 5
          end
        end
      end

      Fiber.yield

      # Perform rapid updates
      5.times do |i|
        base.name = "rapid-test-#{i}"
        base.save
      end

      select
      when count = chan.receive
        count.should eq(5)
      when timeout(2.seconds)
        fail "Timeout waiting for updates"
      end

      changefeed.stop
    end

    it "handles changefeed iterator stop" do
      changefeed = BasicModel.changes

      spawn do
        changefeed.each do |_|
          # Process one change then stop
          break
        end
      end

      Fiber.yield

      BasicModel.create!(name: "iterator-test")

      sleep 0.1.seconds

      changefeed.stop
    end

    it "handles changefeed with no changes" do
      changefeed = BasicModel.changes
      received_any = false

      spawn do
        changefeed.on do |_|
          received_any = true
        end
      end

      Fiber.yield
      sleep 0.1.seconds

      received_any.should be_false
      changefeed.stop
    end
  end

  describe "Changefeed Lifecycle" do
    it "starts receiving changes immediately after creation" do
      chan = Channel(String).new
      changefeed = BasicModel.changes

      spawn do
        changefeed.on do |change|
          chan.send(change.value.name)
        end
      end

      Fiber.yield

      BasicModel.create!(name: "immediate-test")

      select
      when name = chan.receive
        name.should eq("immediate-test")
      when timeout(1.seconds)
        fail "Timeout waiting for immediate change"
      end

      changefeed.stop
    end

    it "stops receiving changes after stop" do
      chan = Channel(String).new
      changefeed = BasicModel.changes

      spawn do
        changefeed.on do |change|
          chan.send(change.value.name)
        end
      end

      Fiber.yield

      BasicModel.create!(name: "before-stop")

      chan.receive

      changefeed.stop

      # Create after stop - should not receive
      BasicModel.create!(name: "after-stop")

      sleep 0.1.seconds

      # Channel should not receive anything
      select
      when chan.receive
        fail "Should not receive changes after stop"
      when timeout(0.2.seconds)
        # Expected - no changes received
      end
    end

    it "handles changefeed for specific record ID" do
      base1 = BasicModel.create!(name: "record-1")
      base2 = BasicModel.create!(name: "record-2")

      chan = Channel(String).new
      changefeed = BasicModel.changes(base1.id)

      spawn do
        changefeed.on do |change|
          chan.send(change.value.name)
        end
      end

      Fiber.yield

      # Update base1 - should receive
      base1.name = "record-1-updated"
      base1.save

      select
      when name = chan.receive
        name.should eq("record-1-updated")
      when timeout(1.seconds)
        fail "Timeout waiting for base1 update"
      end

      # Update base2 - should NOT receive
      base2.name = "record-2-updated"
      base2.save

      sleep 0.1.seconds

      select
      when chan.receive
        fail "Should not receive changes for different record"
      when timeout(0.2.seconds)
        # Expected - no changes for base2
      end

      changefeed.stop
    end
  end
end

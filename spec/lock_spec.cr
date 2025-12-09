require "timecop"
require "uuid"

require "./spec_helper"

module PgORM
  describe PgORM::PgAdvisoryLock do
    it "locks with try_lock" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)
      lock2 = PgAdvisoryLock.new(id)

      lock1.try_lock.should be_true
      lock2.try_lock.should be_false
      lock1.unlock
      lock2.try_lock.should be_true
    end

    it "locks with synchronize and a block" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)
      lock2 = PgAdvisoryLock.new(id)

      lock1.synchronize do
        lock2.try_lock.should be_false
      end
      lock2.try_lock.should be_true
    end

    it "lock/unlock methods return nil" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)

      lock1.lock.should be_nil
      lock1.unlock.should be_nil
    end

    it "prevents locking twice" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)

      lock1.lock
      expect_raises(Error::LockInvalidOp, /already locked/) { lock1.lock }
    end

    it "prevents unlocking an unlocked lock" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)
      expect_raises(Error::LockInvalidOp, /not locked/) { lock1.unlock }
    end

    it "times out if it cannot get the lock" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)
      lock2 = PgAdvisoryLock.new(id)
      lock1.lock(timeout: 10.seconds)
      expect_raises(Error::LockUnavailable) { lock2.lock(timeout: 0.5.seconds) }
    end

    it "does not timeout if it can get the lock" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)
      lock1.lock(timeout: 0.1.seconds)
      lock1.unlock
      lock1.lock(timeout: 0.seconds)
      lock1.unlock
    end

    it "handles concurrent lock attempts from different locks" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)
      lock2 = PgAdvisoryLock.new(id)
      lock3 = PgAdvisoryLock.new(id)

      lock1.lock
      lock2.try_lock.should be_false
      lock3.try_lock.should be_false
      lock1.unlock

      # Now one of them should be able to get it
      lock2.try_lock.should be_true
      lock3.try_lock.should be_false
      lock2.unlock
    end

    it "releases lock on exception in synchronize block" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)
      lock2 = PgAdvisoryLock.new(id)

      begin
        lock1.synchronize do
          lock2.try_lock.should be_false
          raise "Test exception"
        end
      rescue
        # Exception caught
      end

      # Lock should be released after exception
      lock2.try_lock.should be_true
      lock2.unlock
    end

    it "handles multiple locks with different IDs" do
      id1 = UUID.random.to_s
      id2 = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id1)
      lock2 = PgAdvisoryLock.new(id2)

      lock1.lock
      lock2.lock # Should succeed - different ID
      lock1.unlock
      lock2.unlock
    end

    it "handles lock timeout with custom duration" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)
      lock2 = PgAdvisoryLock.new(id)

      lock1.lock

      start_time = Time.monotonic
      expect_raises(Error::LockUnavailable) do
        lock2.lock(timeout: 0.2.seconds)
      end
      elapsed = Time.monotonic - start_time

      # Should timeout around 0.2 seconds (with some tolerance)
      elapsed.should be_close(0.2.seconds, 0.1.seconds)

      lock1.unlock
    end

    it "can reuse same lock object after unlock" do
      id = UUID.random.to_s
      lock = PgAdvisoryLock.new(id)

      lock.lock
      lock.unlock
      lock.lock
      lock.unlock
      lock.try_lock.should be_true
      lock.unlock
    end

    it "synchronize returns block value" do
      id = UUID.random.to_s
      lock = PgAdvisoryLock.new(id)

      result = lock.synchronize do
        "test_value"
      end

      result.should eq("test_value")
    end

    it "handles zero timeout" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)
      lock2 = PgAdvisoryLock.new(id)

      lock1.lock
      expect_raises(Error::LockUnavailable) do
        lock2.lock(timeout: 0.seconds)
      end
      lock1.unlock
    end

    it "handles concurrent lock attempts with spawn" do
      id = UUID.random.to_s
      lock1 = PgAdvisoryLock.new(id)
      lock2 = PgAdvisoryLock.new(id)
      lock3 = PgAdvisoryLock.new(id)

      results = Channel(Bool).new

      # First lock succeeds
      lock1.lock

      # Spawn multiple concurrent attempts
      spawn do
        results.send(lock2.try_lock)
      end

      spawn do
        results.send(lock3.try_lock)
      end

      # Both should fail
      results.receive.should be_false
      results.receive.should be_false

      lock1.unlock

      # Now one should succeed
      lock2.try_lock.should be_true
      lock2.unlock
    end

    it "prevents deadlock with proper lock ordering" do
      id1 = UUID.random.to_s
      id2 = UUID.random.to_s

      # Always lock in same order to prevent deadlock
      ids = [id1, id2].sort

      lock1a = PgAdvisoryLock.new(ids[0])
      lock2a = PgAdvisoryLock.new(ids[1])
      lock1b = PgAdvisoryLock.new(ids[0])
      lock2b = PgAdvisoryLock.new(ids[1])

      channel = Channel(String).new

      spawn do
        lock1a.lock
        sleep 0.01.seconds
        lock2a.lock(timeout: 1.seconds)
        channel.send("thread1_success")
        lock2a.unlock
        lock1a.unlock
      end

      spawn do
        sleep 0.005.seconds # Let thread1 get first lock
        lock1b.lock(timeout: 1.seconds)
        lock2b.lock(timeout: 1.seconds)
        channel.send("thread2_success")
        lock2b.unlock
        lock1b.unlock
      end

      # Both should succeed with proper ordering
      result1 = channel.receive
      result2 = channel.receive

      result1.should eq("thread1_success")
      result2.should eq("thread2_success")
    end
  end
end

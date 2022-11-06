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
  end
end

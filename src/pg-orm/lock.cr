require "json"
require "yaml"
require "digest/sha1"
require "./settings"

module PgORM
  # PostgreSQL advisory locks for distributed locking and synchronization.
  #
  # Advisory locks are application-level locks that use PostgreSQL's locking
  # infrastructure. They're useful for:
  # - Preventing concurrent execution of critical sections
  # - Distributed job processing (ensure only one worker processes a job)
  # - Rate limiting
  # - Preventing duplicate operations
  #
  # ## Features
  #
  # - **Session-level locks**: Automatically released when connection closes
  # - **Timeout support**: Configurable timeout with exponential backoff
  # - **Named locks**: Use string keys instead of numeric IDs
  # - **Fiber-safe**: Works correctly with Crystal's concurrency model
  #
  # ## Basic Usage
  #
  # ```
  # # Create a lock with a string key
  # lock = PgORM::PgAdvisoryLock.new("process_payments")
  #
  # # Synchronize a block of code
  # lock.synchronize do
  #   # Only one fiber/process can execute this at a time
  #   process_payments()
  # end
  #
  # # Manual lock/unlock
  # lock.lock
  # begin
  #   process_payments()
  # ensure
  #   lock.unlock
  # end
  # ```
  #
  # ## With Timeout
  #
  # ```
  # lock = PgORM::PgAdvisoryLock.new("critical_section")
  # lock.timeout = 10.seconds
  #
  # begin
  #   lock.synchronize do
  #     # Critical section
  #   end
  # rescue PgORM::Error::LockUnavailable
  #   puts "Could not acquire lock within 10 seconds"
  # end
  # ```
  #
  # ## Try Lock (Non-blocking)
  #
  # ```
  # lock = PgORM::PgAdvisoryLock.new("optional_task")
  #
  # if lock.try_lock
  #   begin
  #     # Got the lock, do work
  #     perform_task()
  #   ensure
  #     lock.unlock
  #   end
  # else
  #   puts "Lock already held, skipping task"
  # end
  # ```
  #
  # ## How It Works
  #
  # The lock key (string) is hashed using SHA1 and converted to a 64-bit integer
  # that PostgreSQL's advisory lock functions can use. This means:
  # - Same key always produces the same lock ID
  # - Different keys are extremely unlikely to collide
  # - Locks are visible in `pg_locks` system view
  class PgAdvisoryLock
    include JSON::Serializable
    include YAML::Serializable

    extend ::PgORM::Settings

    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    @pg_key : Int64? = nil

    # Lock acquisition timeout (defaults to 5 seconds from settings)
    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    property timeout : Time::Span = PgAdvisoryLock.settings.lock_timeout

    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    getter? locked : Bool = false

    # Creates a new advisory lock with the given key.
    #
    # ## Parameters
    #
    # - `key`: String identifier for the lock (will be hashed to Int64)
    # - `timeout`: Optional timeout for lock acquisition (defaults to 5 seconds)
    #
    # ## Example
    #
    # ```
    # lock = PgORM::PgAdvisoryLock.new("process_payments")
    # lock = PgORM::PgAdvisoryLock.new("critical_section", timeout: 10.seconds)
    # ```
    def initialize(@key : String, timeout : Time::Span? = nil)
      @timeout = timeout if timeout
    end

    # Acquires the lock, executes the block, and releases the lock.
    #
    # This is the recommended way to use advisory locks as it ensures
    # the lock is always released, even if an exception occurs.
    #
    # ## Example
    #
    # ```
    # lock = PgORM::PgAdvisoryLock.new("process_payments")
    # lock.synchronize do
    #   # Only one fiber/process can execute this at a time
    #   Payment.process_pending
    # end
    # ```
    #
    # Raises `Error::LockUnavailable` if the lock cannot be acquired within the timeout.
    def synchronize(**options, &)
      lock(**options)
      begin
        yield
      ensure
        unlock if locked?
      end
    end

    # Acquires the lock, waiting up to the specified timeout.
    #
    # Uses exponential backoff (starting at 0.1s, doubling up to 1s) to
    # reduce database load while waiting.
    #
    # ## Example
    #
    # ```
    # lock = PgORM::PgAdvisoryLock.new("my_lock")
    # lock.lock(timeout: 10.seconds)
    # begin
    #   # Critical section
    # ensure
    #   lock.unlock
    # end
    # ```
    #
    # Raises `Error::LockUnavailable` if the lock cannot be acquired within the timeout.
    def lock(timeout : Time::Span = self.timeout) : Nil
      sleep_amount = 0.1.seconds
      start_at = Time.utc
      loop do
        return if try_lock

        raise Error::LockUnavailable.new(@key) if Time.utc - start_at + sleep_amount > timeout
        sleep(sleep_amount)

        sleep_amount = {1.seconds, sleep_amount * 2}.min
      end
    end

    # Attempts to acquire the lock without waiting.
    #
    # Returns true if the lock was acquired, false if it's already held.
    # This is useful when you want to skip work if the lock is unavailable.
    #
    # ## Example
    #
    # ```
    # lock = PgORM::PgAdvisoryLock.new("optional_task")
    # if lock.try_lock
    #   begin
    #     perform_task()
    #   ensure
    #     lock.unlock
    #   end
    # else
    #   puts "Lock already held, skipping"
    # end
    # ```
    #
    # Raises `Error::LockInvalidOp` if the lock is already held by this instance.
    def try_lock
      raise Error::LockInvalidOp.new(locked: locked?, key: @key) if locked?
      @locked = pg_lock_available?
      pg_lock if @locked
      @locked
    end

    # Releases the lock.
    #
    # ## Example
    #
    # ```
    # lock.lock
    # begin
    #   # Critical section
    # ensure
    #   lock.unlock
    # end
    # ```
    #
    # Raises `Error::LockInvalidOp` if the lock is not currently held.
    def unlock : Nil
      raise Error::LockInvalidOp.new(locked: locked?, key: @key) unless locked?
      pg_release
      @locked = false
    end

    # Returns the number of advisory locks currently held in the database.
    #
    # Useful for monitoring and debugging.
    #
    # ## Example
    #
    # ```
    # puts "Active advisory locks: #{PgORM::PgAdvisoryLock.count}"
    # ```
    def self.count
      Database.with_connection do |db|
        db.scalar("select * from pg_locks where locktype = 'advisory'").as(Int64)
      end
    end

    private def pg_lock : Nil
      ::PgORM::Database.with_connection do |db|
        db.exec "SELECT pg_advisory_lock(#{pg_key})"
      end
    end

    private def pg_lock_available?
      # Always use new connection to check for lock
      DB.connect(Settings.to_uri) do |db|
        db.scalar("SELECT pg_try_advisory_lock(#{pg_key})").as(Bool)
      end
    end

    private def pg_release
      Database.with_connection do |db|
        db.scalar("SELECT pg_advisory_unlock(#{pg_key})").as(Bool)
      end
    end

    private def pg_key
      @pg_key ||= begin
        bigint_bytes = 8
        pg_bigint_range = 2 &** (bigint_bytes * 8)
        hex_digits = 2 * bigint_bytes
        Digest::SHA1.hexdigest(@key)[0...hex_digits - 1].to_i64(16) &- pg_bigint_range // 2
      end
    end
  end
end

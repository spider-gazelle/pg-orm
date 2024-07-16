require "json"
require "yaml"
require "digest/sha1"
require "./settings"

module PgORM
  # Postgresql Advisory Locks for ::PgORM
  class PgAdvisoryLock
    include JSON::Serializable
    include YAML::Serializable

    extend ::PgORM::Settings

    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    @pg_key : Int64? = nil

    # Lock acquisition timeout
    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    property timeout : Time::Span = PgAdvisoryLock.settings.lock_timeout

    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    getter? locked : Bool = false

    def initialize(@key : String, timeout : Time::Span? = nil)
      @timeout = timeout if timeout
    end

    def synchronize(**options, &)
      lock(**options)
      begin
        yield
      ensure
        unlock if locked?
      end
    end

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

    def try_lock
      raise Error::LockInvalidOp.new(locked: locked?, key: @key) if locked?
      @locked = pg_lock_available?
      pg_lock if @locked
      @locked
    end

    def unlock : Nil
      raise Error::LockInvalidOp.new(locked: locked?, key: @key) unless locked?
      pg_release
      @locked = false
    end

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

module PgORM
  # Base exception class for all PgORM errors.
  class Error < Exception
    getter message

    def initialize(@message : String? = "")
      super(message)
    end

    # Raised when attempting to use a changefeed that has been closed.
    #
    # ## Example
    #
    # ```
    # feed = User.changes
    # feed.stop
    # feed.next # Raises ChangefeedClosed
    # ```
    class ChangefeedClosed < Error
    end

    # Raised when attempting to modify a read-only record.
    #
    # Records with read-only attributes cannot be saved if those
    # attributes are modified.
    class ReadOnlyRecord < Error
    end

    # Raised when a record already exists (duplicate key violation).
    class RecordExists < Error
    end

    # Raised when a record cannot be found in the database.
    #
    # ## Example
    #
    # ```
    # User.find(999999)                              # Raises RecordNotFound
    # User.find!(999999)                             # Raises RecordNotFound
    # User.find_by(email: "nonexistent@example.com") # Raises RecordNotFound
    # ```
    #
    # Use the `?` variants to return nil instead:
    #
    # ```
    # User.find?(999999)                              # => nil
    # User.find_by?(email: "nonexistent@example.com") # => nil
    # ```
    class RecordNotFound < Error
    end

    # Raised when a record fails validation.
    #
    # This exception includes the model instance and all validation errors.
    #
    # ## Example
    #
    # ```
    # user = User.new(name: "")
    # begin
    #   user.save!
    # rescue PgORM::Error::RecordInvalid => e
    #   puts e.message # => "User has an invalid field. `name` can't be blank"
    #   e.errors.each do |error|
    #     puts "#{error[:field]}: #{error[:message]}"
    #   end
    # end
    # ```
    class RecordInvalid < Error
      getter model : Base
      getter errors : Array(NamedTuple(field: Symbol, message: String))

      def initialize(@model, message = nil)
        @errors = @model.errors.map do |e|
          {
            field:   e.field,
            message: e.message,
          }
        end

        message = build_message if message.nil?
        super(message)
      end

      protected def build_message
        String.build do |io|
          remaining = errors.size
          io << @model.class.to_s << ' ' << (remaining > 1 ? "has invalid fields." : "has an invalid field.") << ' '
          errors.each do |error|
            remaining -= 1
            io << '`' << error[:field].to_s << '`'
            io << " " << error[:message]
            io << ", " unless remaining.zero?
          end
        end
      end
    end

    # Raised when a record cannot be saved to the database.
    #
    # This can occur when:
    # - Attempting to save a destroyed record
    # - Database constraints are violated
    # - The database operation fails
    #
    # ## Example
    #
    # ```
    # user = User.find(1)
    # user.destroy
    # user.save! # Raises RecordNotSaved: "Cannot save a destroyed record!"
    # ```
    class RecordNotSaved < Error
    end

    # Raised when a database operation fails.
    #
    # This is a general database error that doesn't fit into more
    # specific error categories.
    class DatabaseError < Error
    end

    # Raised when the database connection cannot be established.
    #
    # ## Example
    #
    # ```
    # PgORM::Database.configure do |settings|
    #   settings.host = "nonexistent.example.com"
    # end
    # User.all.to_a # Raises ConnectError
    # ```
    class ConnectError < Error
    end

    # Raised when an invalid lock operation is attempted.
    #
    # This occurs when:
    # - Trying to lock an already locked lock
    # - Trying to unlock a lock that isn't locked
    #
    # ## Example
    #
    # ```
    # lock = PgORM::PgAdvisoryLock.new("my_lock")
    # lock.lock
    # lock.lock # Raises LockInvalidOp: "Lock (my_lock) already locked"
    # ```
    class LockInvalidOp < Error
      def initialize(key : String, locked : Bool)
        super("Lock (#{key}) #{locked ? "already" : "not"} locked")
      end
    end

    # Raised when an advisory lock cannot be acquired within the timeout.
    #
    # ## Example
    #
    # ```
    # lock = PgORM::PgAdvisoryLock.new("my_lock")
    # lock.timeout = 1.second
    #
    # # If another process holds the lock for > 1 second
    # lock.lock # Raises LockUnavailable: "Lock (my_lock) unavailable"
    # ```
    class LockUnavailable < Error
      def initialize(key : String)
        super("Lock (#{key}) unavailable")
      end
    end
  end
end

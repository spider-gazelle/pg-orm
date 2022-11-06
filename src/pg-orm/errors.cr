module PgORM
  class Error < Exception
    getter message

    def initialize(@message : String? = "")
      super(message)
    end

    class ChangefeedClosed < Error
    end

    class ReadOnlyRecord < Error
    end

    class RecordExists < Error
    end

    class RecordNotFound < Error
    end

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

    class RecordNotSaved < Error
    end

    class DatabaseError < Error
    end

    class ConnectError < Error
    end

    class LockInvalidOp < Error
      def initialize(key : String, locked : Bool)
        super("Lock (#{key}) #{locked ? "already" : "not"} locked")
      end
    end

    class LockUnavailable < Error
      def initialize(key : String)
        super("Lock (#{key}) unavailable")
      end
    end
  end
end

module PgORM
  # Persistence module handles all database CRUD (Create, Read, Update, Delete) operations.
  #
  # This module is automatically included in `PgORM::Base` and provides instance methods
  # for saving, updating, and deleting records, as well as class methods for bulk operations.
  #
  # ## Lifecycle States
  #
  # Records can be in one of three states:
  # - **New**: Not yet saved to database (`new_record? == true`)
  # - **Persisted**: Saved to database (`persisted? == true`)
  # - **Destroyed**: Deleted from database (`destroyed? == true`)
  #
  # ## Callbacks
  #
  # Persistence operations trigger callbacks in this order:
  # - **Create**: before_create → before_save → after_save → after_create
  # - **Update**: before_update → before_save → after_save → after_update
  # - **Destroy**: before_destroy → after_destroy
  module Persistence
    # :nodoc:
    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    getter? new_record : Bool = true

    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    protected setter new_record : Bool

    # Returns true if the record has been saved to the database and not destroyed.
    #
    # ## Example
    #
    # ```
    # user = User.new(name: "John")
    # user.persisted? # => false
    #
    # user.save!
    # user.persisted? # => true
    #
    # user.destroy
    # user.persisted? # => false
    # ```
    def persisted?
      !(new_record? || destroyed?)
    end

    # :nodoc:
    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    property? destroyed : Bool = false

    macro included
      # Creates and saves a new record to the database.
      #
      # Raises an exception if validation fails or the record cannot be saved.
      #
      # ## Example
      #
      # ```
      # user = User.create!(name: "John", email: "john@example.com")
      # # => #<User id: 1, name: "John", email: "john@example.com">
      #
      # # Raises PgORM::Error::RecordInvalid if validation fails
      # User.create!(name: "") # => Error!
      # ```
      #
      # See also: `#save!`
      def self.create!(**attributes)
        new(**attributes).save!
      end

      # Creates and attempts to save a new record to the database.
      #
      # Returns the record regardless of whether it was saved successfully.
      # Check `#persisted?` to confirm if the save succeeded.
      #
      # ## Example
      #
      # ```
      # user = User.create(name: "John", email: "john@example.com")
      # if user.persisted?
      #   puts "User created with ID: #{user.id}"
      # else
      #   puts "Failed to create user: #{user.errors}"
      # end
      # ```
      #
      # See also: `#save`
      def self.create(**attributes)
        record = new(**attributes)
        begin
          record.save!
        end
        record
      end

      # Removes all records from the table using DELETE.
      #
      # This is slower than `truncate` but respects foreign key constraints
      # and triggers any database-level triggers.
      #
      # ## Example
      #
      # ```
      # User.clear # DELETE FROM users
      # ```
      #
      # **Warning**: This deletes all data! Use with caution.
      def self.clear
        Database.with_connection do |db|
          db.exec "DELETE FROM #{Database.quote(self.table_name)}"
        end
      end

      # Quickly removes all records from the table using TRUNCATE.
      #
      # Much faster than `clear` for large tables, but:
      # - Requires table-level locks
      # - Resets auto-increment sequences
      # - Can cascade to related tables if `cascade: true`
      #
      # ## Example
      #
      # ```
      # # Truncate just this table
      # User.truncate(cascade: false)
      #
      # # Truncate and cascade to related tables
      # User.truncate(cascade: true) # Also truncates related records
      # ```
      #
      # **Warning**: This deletes all data! Use with caution.
      def self.truncate(cascade = true)
        casc = cascade ? "CASCADE" : ""
        Database.with_connection do |db|
          db.exec "TRUNCATE #{Database.quote(self.table_name)}#{casc}"
        end
      end

      # Updates one or more records by ID without loading them into memory.
      #
      # This is more efficient than loading, modifying, and saving records.
      # Does not run validations or callbacks.
      #
      # ## Example
      #
      # ```
      # # Update single record
      # User.update(1, {name: "John Updated"})
      # User.update(1, name: "John Updated")
      #
      # # Update multiple records by ID
      # User.update([1, 2, 3], {active: false})
      #
      # # Update with composite primary key
      # CompositeModel.update({key1: "a", key2: 1}, {status: "active"})
      # ```
      def self.update(id : Value, args) : Nil
        case key = primary_key
        when Symbol
          where({key => id}).update_all(args)
        else
          raise ArgumentError.new("must provide multiple id values for composite primary keys")
        end
      end

      # Updates multiple records by an array of IDs.
      #
      # ## Example
      #
      # ```
      # User.update([1, 2, 3], {active: false})
      # ```
      def self.update(id : Enumerable(Value), args) : Nil
        case key = primary_key
        when Symbol
          where({key => id.to_a}).update_all(args)
        when Tuple
          where(key.zip(id.to_a).to_h).update_all(args)
        end
      end

      # Updates multiple records with composite primary keys.
      #
      # ## Example
      #
      # ```
      # CompositeModel.update([{key1: "a", key2: 1}, {key1: "b", key2: 2}], {status: "active"})
      # ```
      def self.update(id : Enumerable(Enumerable(Value)), args) : Nil
        case keys = primary_key
        when Tuple
          find_all(id).update_all(args)
        else
          raise ArgumentError.new("multiple id values are only supported for composite primary keys")
        end
      end

      # Updates records using keyword arguments.
      #
      # ## Example
      #
      # ```
      # User.update(1, name: "John", active: true)
      # ```
      def self.update(id, **args) : Nil
        update(id, args)
      end

      # Deletes multiple records by an array of IDs.
      #
      # Does not load records into memory or run callbacks.
      # Use `#destroy` if you need callbacks.
      #
      # ## Example
      #
      # ```
      # User.delete([1, 2, 3, 4, 5])
      # ```
      def self.delete(ids : Enumerable(Value))
        case keys = primary_key
        when Symbol
          where({keys => ids.to_a}).delete_all
        else
          delete({ids})
        end
      end

      # Deletes multiple records with composite primary keys.
      #
      # ## Example
      #
      # ```
      # CompositeModel.delete([{key1: "a", key2: 1}, {key1: "b", key2: 2}])
      # ```
      def self.delete(ids : Enumerable(Enumerable(Value)))
        case keys = primary_key
        when Tuple
          find_all(ids).delete_all
        else
          raise ArgumentError.new("multiple id values are only supported for composite primary keys")
        end
      end

      # Deletes one or more records by ID.
      #
      # Does not load records into memory or run callbacks.
      # More efficient than `destroy` but doesn't trigger callbacks or update associations.
      #
      # ## Example
      #
      # ```
      # # Delete single record
      # User.delete(1)
      #
      # # Delete multiple records
      # User.delete(1, 2, 3, 4, 5)
      # ```
      def self.delete(*ids) : Nil
        delete(ids)
      end
    end

    # Saves the record to the database.
    #
    # For new records, performs an INSERT. For existing records, performs an UPDATE.
    # Returns true if successful, false if validation fails.
    #
    # ## Example
    #
    # ```
    # user = User.new(name: "John")
    # if user.save
    #   puts "Saved! ID: #{user.id}"
    # else
    #   puts "Failed: #{user.errors}"
    # end
    #
    # # Update existing record
    # user.name = "Jane"
    # user.save # => true (if valid)
    # ```
    #
    # ## Callbacks
    #
    # Triggers appropriate callbacks based on record state:
    # - New records: before_create, before_save, after_save, after_create
    # - Existing: before_update, before_save, after_save, after_update
    def save(**options)
      save!(**options)
      true
    rescue ::PgORM::Error
      false
    end

    # Saves the record to the database, raising an exception on failure.
    #
    # For new records, performs an INSERT. For existing records, performs an UPDATE.
    # Only updates changed attributes (dirty tracking).
    #
    # ## Example
    #
    # ```
    # user = User.new(name: "John")
    # user.save! # => #<User id: 1, name: "John">
    #
    # user.name = "Jane"
    # user.save! # Only updates 'name' column
    # ```
    #
    # ## Raises
    #
    # - `PgORM::Error::RecordNotSaved` if record was destroyed
    # - `PgORM::Error::RecordInvalid` if validation fails
    # - `PgORM::Error::RecordNotSaved` if database operation fails
    def save!(**options)
      raise ::PgORM::Error::RecordNotSaved.new("Cannot save a destroyed record!") if destroyed?
      persisted? ? __update(**options) : __create(**options)
    end

    # Updates the record with new attributes and saves it.
    #
    # Returns true if successful, false if validation fails.
    #
    # ## Example
    #
    # ```
    # user = User.find(1)
    # if user.update(name: "Jane", email: "jane@example.com")
    #   puts "Updated!"
    # else
    #   puts "Failed: #{user.errors}"
    # end
    # ```
    def update(**attributes)
      update!(**attributes)
      true
      # rescue ::PgORM::Error
      #   false
    end

    # Updates the record with new attributes and saves it, raising on failure.
    #
    # ## Example
    #
    # ```
    # user = User.find(1)
    # user.update!(name: "Jane", email: "jane@example.com")
    # ```
    #
    # Raises `PgORM::Error::RecordInvalid` if validation fails.
    def update!(**attributes)
      assign_attributes(**attributes)
      save!
    end

    # Atomically updates specific fields without running callbacks or validations.
    #
    # This is faster than `update!` but bypasses:
    # - Validations
    # - Callbacks (before_save, after_save, etc.)
    # - Dirty tracking (changes are cleared after update)
    #
    # Use this for performance-critical updates where you don't need the full
    # persistence lifecycle.
    #
    # ## Example
    #
    # ```
    # user = User.find(1)
    # user.update_fields(last_login: Time.utc, login_count: 42)
    # # Direct UPDATE query, no callbacks
    # ```
    #
    # Raises `Error::RecordNotSaved` if called on a new record.
    def update_fields(**attributes)
      raise Error::RecordNotSaved.new("Cannot update fields of a new record!") if new_record?
      assign_attributes(**attributes)
      self.class.update(self.id, attributes)
      clear_changes_information
      self
    end

    # Destroys the record, running callbacks and updating associations.
    #
    # This is the "safe" way to delete records as it:
    # - Runs before_destroy and after_destroy callbacks
    # - Handles dependent associations (nullify, delete, destroy)
    # - Wraps everything in a transaction
    #
    # ## Example
    #
    # ```
    # user = User.find(1)
    # user.destroy
    # user.destroyed? # => true
    # user.persisted? # => false
    # ```
    #
    # Returns self (even if already destroyed or new).
    def destroy
      return self if destroyed?
      return self if new_record?

      Database.transaction do
        run_destroy_callbacks do
          __delete
          self
        end
      end
    end

    # Deletes the record from the database without callbacks.
    #
    # This is faster than `destroy` but:
    # - Does NOT run callbacks
    # - Does NOT update associations
    # - Does NOT wrap in a transaction
    #
    # Use this for performance-critical deletions where you don't need
    # the full destroy lifecycle.
    #
    # ## Example
    #
    # ```
    # user = User.find(1)
    # user.delete # Direct DELETE query
    # ```
    #
    # Returns self (even if already destroyed or new).
    def delete
      return self if destroyed?
      return self if new_record?

      __delete
    end

    # Reloads the record from the database, discarding any changes.
    #
    # Useful for:
    # - Reverting unsaved changes
    # - Getting fresh data after external updates
    # - Clearing dirty tracking
    #
    # ## Example
    #
    # ```
    # user = User.find(1)
    # user.name = "Changed"
    # user.reload!  # Reverts to database value
    # user.changed? # => false
    # ```
    #
    # ## Raises
    #
    # - `PgORM::Error::RecordNotSaved` if record was never persisted
    # - `PgORM::Error::RecordNotFound` if record no longer exists in database
    def reload!
      raise ::PgORM::Error::RecordNotSaved.new("Cannot reload unpersisted record") unless persisted?

      builder = Query::Builder.new(self.class.table_name)
        .where!(self.primary_key_hash)
        .limit!(1)

      found = Database.adapter(builder).select_one do |rs|
        load_attributes(rs)
        true
      end

      raise ::PgORM::Error::RecordNotFound.new("Key not present: #{id}") unless found

      clear_changes_information
      self
    end

    # Internal update function, runs callbacks and pushes update to DB
    #
    private def __update(**options)
      Database.transaction do
        run_update_callbacks do
          run_save_callbacks do
            raise ::PgORM::Error::RecordInvalid.new(self) unless valid?
            if changed?
              self.class.update(id, self.changed_persist_attributes) # self.changed_attributes)
            end
            clear_changes_information
          end
        end
      end
      self
    end

    # Internal create function, runs callbacks and pushes new model to DB
    #
    private def __create(**options)
      builder = Query::Builder.new(table_name, primary_key.first.to_s)
      adapter = Database.adapter(builder)

      Database.transaction do
        run_create_callbacks do
          run_save_callbacks do
            raise ::PgORM::Error::RecordInvalid.new(self) unless valid?
            attributes = self.persistent_attributes

            # clear primary keys if they are not set to a value (assume auto generated)
            keys = primary_key
            vals = self.id?
            case vals
            when Nil
              keys.each { |key| attributes.delete(key) }
            when Enumerable
              primary_key.each_with_index { |key, index| attributes.delete(key) if vals[index].nil? }
            end

            begin
              adapter.insert(attributes) do |rid|
                set_primary_key_after_create(rid)
                clear_changes_information
                self.new_record = false
              end
            rescue ex : Exception
              raise ::PgORM::Error::RecordNotSaved.new("Failed to create record. Reason: #{ex.message}")
            end
          end
        end
      end
      self
    end

    # Delete record in table, update model metadata
    #
    private def __delete
      keys = self.primary_key
      case ids = self.id
      when Tuple
        self.class.where(keys.zip(ids.to_a).to_h).delete_all
      else
        self.class.where({keys[0] => ids}).delete_all
      end

      @destroyed = true
      clear_changes_information
      true
    end
  end
end

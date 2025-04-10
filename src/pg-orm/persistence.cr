module PgORM
  module Persistence
    # :nodoc:
    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    getter? new_record : Bool = true

    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    protected setter new_record : Bool

    def persisted?
      !(new_record? || destroyed?)
    end

    @[JSON::Field(ignore: true)]
    @[YAML::Field(ignore: true)]
    property? destroyed : Bool = false

    macro included
      # Creates the model.
      #
      # See `#save!`
      def self.create!(**attributes)
        new(**attributes).save!
      end

      # Creates the model.
      #
      # Persistence can be confirmed via `#persisted?`
      def self.create(**attributes)
        record = new(**attributes)
        begin
          record.save!
        end
        record
      end

      # Removes all records from the table
      #
      def self.clear
        Database.with_connection do |db|
          db.exec "DELETE FROM #{Database.quote(self.table_name)}"
        end
      end

      # Truncate quickly removes all from table.
      def self.truncate(cascade = true)
        casc = cascade ? "CASCADE" : ""
        Database.with_connection do |db|
          db.exec "TRUNCATE #{Database.quote(self.table_name)}#{casc}"
        end
      end

      # Updates one or many records identified by *id* in the database.
      #
      # ```
      # User.update(1, {name: user})
      # User.update([1, 2, 3], {group_id: 2})
      # ```
      def self.update(id : Value, args) : Nil
        case key = primary_key
        when Symbol
          where({key => id}).update_all(args)
        else
          raise ArgumentError.new("must provide multiple id values for composite primary keys")
        end
      end

      def self.update(id : Enumerable(Value), args) : Nil
        case key = primary_key
        when Symbol
          where({key => id.to_a}).update_all(args)
        when Tuple
          where(key.zip(id.to_a).to_h).update_all(args)
        end
      end

      def self.update(id : Enumerable(Enumerable(Value)), args) : Nil
        case keys = primary_key
        when Tuple
          # TODO:: Optimise this using (primary1, primary2) IN ((val1, val2), (val3, val4))
          id.each do |id_tuple|
            where(keys.zip(id_tuple.to_a).to_h).update_all(args)
          end
        else
          raise ArgumentError.new("multiple id values are only supported for composite primary keys")
        end
      end

      # :ditto:
      def self.update(id, **args) : Nil
        update(id, args)
      end

      def self.delete(ids : Enumerable(Value))
        case keys = primary_key
        when Symbol
          where({keys => ids.to_a}).delete_all
        else
          delete({ids})
        end
      end

      def self.delete(ids : Enumerable(Enumerable(Value)))
        case keys = primary_key
        when Tuple
          # TODO:: Optimise this using (primary1, primary2) IN ((val1, val2), (val3, val4))
          ids.each do |id_tuple|
            where(keys.zip(id_tuple.to_a).to_h).delete_all
          end
        else
          raise ArgumentError.new("multiple id values are only supported for composite primary keys")
        end
      end

      # Deletes one or many records identified by *ids* from the database.
      #
      # ```
      # User.delete(1)
      # User.delete(1, 2, 3)
      # ```
      def self.delete(*ids) : Nil
        delete(ids)
      end
    end

    # Saves the model.
    #
    # If the model is new, a record gets created in the database, otherwise
    # the existing record gets updated.
    def save(**options)
      save!(**options)
      true
    rescue ::PgORM::Error
      false
    end

    # Saves the model.
    #
    # If the model is new, a record gets created in the database, otherwise
    # the existing record gets updated.
    #
    # Raises
    # - `PgORM::Error:RecordNotSaved` if was record was destroyed before save
    # - `PgORM::Error:RecordNotSaved` if was record was not saved by DB
    # - `PgORM::Error:RecordInvalid` on validation failures
    def save!(**options)
      raise ::PgORM::Error::RecordNotSaved.new("Cannot save a destroyed record!") if destroyed?
      persisted? ? __update(**options) : __create(**options)
    end

    # Updates the model
    #
    # Non-atomic updates are required for multirecord updates
    def update(**attributes)
      update!(**attributes)
      true
      # rescue ::PgORM::Error
      #   false
    end

    # Updates the model in place
    #
    # Raises `PgORM::Error::RecordInvalid` on update failure
    def update!(**attributes)
      assign_attributes(**attributes)
      save!
    end

    # Atomically update specified fields, without running callbacks
    #
    def update_fields(**attributes)
      raise Error::RecordNotSaved.new("Cannot update fields of a new record!") if new_record?
      assign_attributes(**attributes)
      self.class.update(self.id, attributes)
      clear_changes_information
      self
    end

    # Destroy object, run destroy callbacks and update associations
    #
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

    # Only deletes record from table. No callbacks or updated associations
    #
    def delete
      return self if destroyed?
      return self if new_record?

      __delete
    end

    # Reload the model in place.
    #
    # Raises
    # - `PgORM::Error::RecordNotSaved` if record was not previously persisted
    # - `PgORM::Error::RecordNotFound` if record fails to load
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
      Database.with_connection do |db|
        db.exec "DELETE FROM #{Database.quote(self.table_name)} WHERE #{self.class.query_primary_key}", id
      end
      @destroyed = true
      clear_changes_information
      true
    end
  end
end

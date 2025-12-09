require "time"

# Automatically manages created_at and updated_at timestamp columns.
#
# Include this module in your model to automatically track when records
# are created and updated. The timestamps are managed via callbacks:
# - `created_at` is set when the record is first saved
# - `updated_at` is set on every save (create and update)
#
# ## Usage
#
# ```
# class User < PgORM::Base
#   include PgORM::Timestamps
#
#   attribute id : Int64
#   attribute name : String
#   # created_at and updated_at are added automatically
# end
#
# user = User.create!(name: "John")
# user.created_at # => 2024-01-15 10:30:00 UTC
# user.updated_at # => 2024-01-15 10:30:00 UTC
#
# sleep 5
# user.update!(name: "Jane")
# user.created_at # => 2024-01-15 10:30:00 UTC (unchanged)
# user.updated_at # => 2024-01-15 10:30:05 UTC (updated)
# ```
#
# ## Database Schema
#
# Make sure your table has these columns:
#
# ```sql
# CREATE TABLE users (
#   id BIGSERIAL PRIMARY KEY,
#   name TEXT NOT NULL,
#   created_at TIMESTAMP NOT NULL,
#   updated_at TIMESTAMP NOT NULL
# );
# ```
module PgORM::Timestamps
  macro included
    attribute created_at : Time = -> { Time.utc }
    attribute updated_at : Time = -> { Time.utc }

    before_create do
      self.created_at = self.updated_at = Time.utc
    end

    before_update do
      self.updated_at = Time.utc
    end
  end
end

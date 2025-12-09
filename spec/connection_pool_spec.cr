require "./spec_helper"

describe PgORM::Database do
  describe "Connection Pool" do
    it "handles concurrent connection requests" do
      results = Channel(Bool).new
      fiber_count = 10

      fiber_count.times do
        spawn do
          begin
            PgORM::Database.connection do |db|
              db.query_one("SELECT 1", as: Int32).should eq(1)
            end
            results.send(true)
          rescue ex
            results.send(false)
          end
        end
      end

      # All fibers should successfully get connections
      fiber_count.times do
        results.receive.should be_true
      end
    end

    it "reuses connections from pool" do
      # Execute multiple queries - should reuse connections
      10.times do
        PgORM::Database.connection do |db|
          result = db.query_one("SELECT 1", as: Int32)
          result.should eq(1)
        end
      end
    end

    it "releases connections back to pool" do
      # Get a connection and release it
      PgORM::Database.connection do |db|
        db.query_one("SELECT 1", as: Int32).should eq(1)
      end

      # Should be able to get another connection
      PgORM::Database.connection do |db|
        db.query_one("SELECT 2", as: Int32).should eq(2)
      end
    end

    it "handles connection errors gracefully" do
      expect_raises(Exception) do
        PgORM::Database.connection do |db|
          db.query("SELECT * FROM nonexistent_table")
        end
      end

      # Pool should still work after error
      PgORM::Database.connection do |db|
        db.query_one("SELECT 1", as: Int32).should eq(1)
      end
    end

    it "maintains fiber-local connections in transactions" do
      group = Group.create!(name: "Pool Test Group")
      user = User.create!(uuid: UUID.random, group_id: group.id, name: "Pool Test User")

      PgORM::Database.transaction do
        user.name = "Updated in Transaction"
        user.save!

        # Same fiber should see the update
        found = User.find!(user.uuid)
        found.name.should eq("Updated in Transaction")
      end

      user.destroy
      group.destroy
    end

    it "isolates connections between fibers" do
      group = Group.create!(name: "Fiber Test Group")
      user1 = User.create!(uuid: UUID.random, group_id: group.id, name: "Fiber 1 User")
      user2 = User.create!(uuid: UUID.random, group_id: group.id, name: "Fiber 2 User")

      channel = Channel(String).new

      spawn do
        PgORM::Database.transaction do
          user1.name = "Modified by Fiber 1"
          user1.save!
          sleep 0.05.seconds
        end
        # Send after transaction commits
        channel.send("fiber1_done")
      end

      spawn do
        sleep 0.01.seconds
        PgORM::Database.transaction do
          user2.name = "Modified by Fiber 2"
          user2.save!
        end
        # Send after transaction commits
        channel.send("fiber2_done")
      end

      2.times { channel.receive }

      # Refetch from database to see changes made in other fibers
      User.find!(user1.uuid).name.should eq("Modified by Fiber 1")
      User.find!(user2.uuid).name.should eq("Modified by Fiber 2")

      user1.destroy
      user2.destroy
      group.destroy
    end

    it "handles nested transactions with connection reuse" do
      group = Group.create!(name: "Nested Transaction Group")
      user = User.create!(uuid: UUID.random, group_id: group.id, name: "Nested Transaction User")

      PgORM::Database.transaction do
        user.name = "Outer Transaction"
        user.save!

        PgORM::Database.transaction do
          user.name = "Inner Transaction"
          user.save!
        end

        user.reload!.name.should eq("Inner Transaction")
      end

      user.destroy
      group.destroy
    end

    it "executes queries without explicit connection management" do
      # ORM should handle connections automatically
      users = User.where(name: "Auto Connection Test").to_a
      users.should be_a(Array(User))
    end

    it "handles concurrent queries from multiple fibers" do
      group = Group.create!(name: "Concurrent Test Group")
      results = Channel(Int64).new
      fiber_count = 5

      fiber_count.times do |i|
        spawn do
          user = User.create!(uuid: UUID.random, group_id: group.id, name: "Concurrent User #{i}")
          count = User.where(name: "Concurrent User #{i}").count
          user.destroy
          results.send(count)
        end
      end

      fiber_count.times do
        results.receive.should eq(1_i64)
      end

      group.destroy
    end

    it "maintains connection during query iteration" do
      # Create test users
      group = Group.create!(name: "Iterator Test Group")
      users = 3.times.map { |i| User.create!(uuid: UUID.random, group_id: group.id, name: "Iterator User #{i}") }.to_a

      count = 0
      User.where("name LIKE ?", "Iterator User%").each do |user|
        count += 1
        user.name.should start_with("Iterator User")
      end

      count.should eq(3)

      users.each(&.destroy)
      group.destroy
    end

    it "handles connection pool with transactions and rollbacks" do
      group = Group.create!(name: "Rollback Test Group")
      user = User.create!(uuid: UUID.random, group_id: group.id, name: "Rollback Test")
      original_name = user.name

      begin
        PgORM::Database.transaction do
          user.name = "Should Rollback"
          user.save!
          raise "Intentional Error"
        end
      rescue
        # Expected
      end

      # Connection should be released and usable
      user.reload!.name.should eq(original_name)

      user.destroy
      group.destroy
    end

    it "supports multiple sequential transactions" do
      group = Group.create!(name: "Sequential Transaction Group")
      user = User.create!(uuid: UUID.random, group_id: group.id, name: "Sequential Transactions")

      PgORM::Database.transaction do
        user.name = "Transaction 1"
        user.save!
      end

      PgORM::Database.transaction do
        user.name = "Transaction 2"
        user.save!
      end

      PgORM::Database.transaction do
        user.name = "Transaction 3"
        user.save!
      end

      user.reload!.name.should eq("Transaction 3")

      user.destroy
      group.destroy
    end

    it "handles connection with long-running queries" do
      # Simulate a longer query
      PgORM::Database.connection do |db|
        db.exec("SELECT pg_sleep(0.1)")
        result = db.query_one("SELECT 42", as: Int32)
        result.should eq(42)
      end
    end

    it "executes raw SQL through connection pool" do
      PgORM::Database.connection do |db|
        result = db.query_one("SELECT COUNT(*) FROM users", as: Int64)
        result.should be >= 0
      end
    end

    it "handles connection cleanup on fiber termination" do
      channel = Channel(Bool).new

      spawn do
        begin
          PgORM::Database.connection do |db|
            db.query_one("SELECT 1", as: Int32)
          end
          channel.send(true)
        rescue
          channel.send(false)
        end
      end

      channel.receive.should be_true

      # Pool should still be functional
      PgORM::Database.connection do |db|
        db.query_one("SELECT 1", as: Int32).should eq(1)
      end
    end
  end
end

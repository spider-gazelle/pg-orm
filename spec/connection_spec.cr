require "./spec_helper"

describe PgORM::Persistence do
  it "test methods" do
    SpecConnection.pool.is_a?(DB::Database).should be_true
  end

  it "test using_connection" do
    SpecConnection.with_connection do |db|
      SpecConnection.with_connection do |db2|
        db.should eq(db2)
      end

      SpecConnection.connection do |db3|
        db.should eq(db3)
      end
    end
  end

  it "test connection" do
    SpecConnection.connection do |db1|
      SpecConnection.connection do |db2|
        db1.should_not eq(db2)
      end
    end
  end

  it "test transaction" do
    SpecConnection.transaction do |tx|
      tx.is_a?(DB::Transaction).should be_true
    end
  end

  it "test transaction commit" do
    user = group = nil

    SpecConnection.transaction do
      group = Group.create(name: "A")
      user = User.create(uuid: UUID.random, name: "B", group_id: group.id)
    end

    Group.exists?(group.not_nil!.id).should be_true
    User.exists?(user.not_nil!.uuid).should be_true
  end

  it "test transaction rollback" do
    user = group = nil

    expect_raises(PgORM::Error::RecordInvalid) do
      SpecConnection.transaction do
        group = Group.create(name: "B")
        user = User.create(name: "C")
      end
    end

    Group.exists?(group.not_nil!.id).should be_false
    user.should be_nil
  end

  it "test nested transactions" do
    user1 = user2 = user3 = group = nil

    SpecConnection.transaction do
      group = Group.create(name: "B")

      expect_raises(PgORM::Error::RecordInvalid) do
        SpecConnection.transaction do
          user1 = User.create(uuid: UUID.random, name: "C", group_id: group.id)
          user2 = User.create(name: "C")
        end
      end

      SpecConnection.transaction do
        user3 = User.create(uuid: UUID.random, name: "D", group_id: group.id)
      end

      Group.exists?(group.not_nil!.id).should be_true
      User.exists?(user1.not_nil!.id).should be_false
      user2.should be_nil
      User.exists?(user3.not_nil!.id).should be_true
    end
  end

  it "handles connection reuse within with_connection" do
    connection_ids = [] of UInt64

    SpecConnection.with_connection do |db|
      connection_ids << db.object_id
      SpecConnection.with_connection do |db2|
        connection_ids << db2.object_id
      end
    end

    # Should reuse same connection
    connection_ids[0].should eq(connection_ids[1])
  end

  it "connection method provides database connections" do
    connection_count = 0

    SpecConnection.connection do |conn|
      connection_count += 1
      conn.should_not be_nil
      # Can execute queries on connection
      result = conn.scalar("SELECT 1")
      result.should eq(1)
    end

    connection_count.should eq(1)
  end

  it "transaction rolls back on exception" do
    group = nil

    expect_raises(Exception, "Forced rollback") do
      SpecConnection.transaction do
        group = Group.create(name: "Rollback Test")
        raise Exception.new("Forced rollback")
      end
    end

    # Group should not exist after rollback
    Group.exists?(group.not_nil!.id).should be_false
  end

  it "can perform queries within transaction" do
    group = nil
    count_before = Group.count

    SpecConnection.transaction do
      group = Group.create(name: "Transaction Query Test")
      # Query within transaction should see the new record
      Group.count.should eq(count_before + 1)
    end

    # After commit, record should still exist
    Group.exists?(group.not_nil!.id).should be_true
    Group.count.should eq(count_before + 1)
  end

  it "handles multiple sequential transactions" do
    group1 = Group.create(name: "Seq1")

    SpecConnection.transaction do
      group1.name = "Updated1"
      group1.save
    end

    SpecConnection.transaction do
      group1.reload!
      group1.name.should eq("Updated1")
      group1.name = "Updated2"
      group1.save
    end

    group1.reload!
    group1.name.should eq("Updated2")
  end

  it "connection pool provides connections" do
    pool = SpecConnection.pool
    pool.should be_a(DB::Database)

    # Should be able to execute queries through pool
    result = pool.scalar("SELECT 1")
    result.should eq(1)
  end

  it "handles transaction with no operations" do
    # Empty transaction should not raise error
    SpecConnection.transaction do
      # No operations
    end
  end

  it "nested transaction rollback doesn't affect outer transaction" do
    group = user = nil

    SpecConnection.transaction do
      group = Group.create(name: "Outer")

      begin
        SpecConnection.transaction do
          user = User.create(uuid: UUID.random, name: "Inner", group_id: group.id)
          raise Exception.new("Inner rollback")
        end
      rescue
        # Catch inner exception
      end

      # Outer transaction continues
      Group.exists?(group.id).should be_true
    end

    # Outer transaction committed
    Group.exists?(group.not_nil!.id).should be_true
  end
end

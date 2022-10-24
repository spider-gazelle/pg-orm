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
end

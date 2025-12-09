describe PgORM::Table do
  describe "Composite key tables" do
    Spec.before_each do
      CompositeKeys.clear
    end

    it "exposes primary keys" do
      {:key_one, :key_two}.should eq(CompositeKeys.primary_key)
    end

    it "should perform CRUD operations" do
      comp1 = CompositeKeys.new(key_one: "one", key_two: "two", payload: "payload")
      comp1.save!
      CompositeKeys.exists?({"one", "two"}).should be_true
      CompositeKeys.exists?({"one", "three"}).should be_false

      comp2 = CompositeKeys.new(key_one: "one", key_two: "three", payload: "hello")
      comp2.save!
      CompositeKeys.find_all([{"one", "two"}, {"one", "three"}]).size.should eq 2
      CompositeKeys.find_all([{"one", "three"}]).size.should eq 1

      # ensure find works as expected
      other = CompositeKeys.find!({"one", "two"})
      other.key_one.should eq comp1.key_one
      other.key_two.should eq comp1.key_two
      other.payload.should eq comp1.payload

      comp1.payload = "updated"
      comp1.save!
      other.reload!
      other.payload.should eq comp1.payload

      other.destroy
      CompositeKeys.exists?({"one", "two"}).should be_false

      other = CompositeKeys.find!({"one", "three"})
      other.key_one.should eq comp2.key_one
      other.key_two.should eq comp2.key_two
      other.payload.should eq comp2.payload

      CompositeKeys.find_all([{"one", "three"}]).update_all({payload: "up_all"})
      other.reload!
      other.payload.should eq "up_all"

      CompositeKeys.find_all([{"one", "three"}]).delete_all
      CompositeKeys.exists?({"one", "three"}).should be_false

      expect_raises(PgORM::Error::RecordNotFound) do
        comp2.reload!
      end

      expect_raises(PgORM::Error::RecordNotFound) do
        CompositeKeys.find!({"one", "three"})
      end
    end

    it "should perform various batch operations" do
      comp1 = CompositeKeys.new(key_one: "one", key_two: "two", payload: "payload")
      comp1.save!

      comp2 = CompositeKeys.new(key_one: "one", key_two: "three", payload: "hello")
      comp2.save!

      ids = { {"one", "two"}, {"one", "three"} }

      CompositeKeys.update(ids, {payload: "updated"})
      comp1.reload!
      comp2.reload!
      comp1.payload.should eq "updated"
      comp2.payload.should eq "updated"

      CompositeKeys.delete(ids)

      expect_raises(PgORM::Error::RecordNotFound) do
        comp1.reload!
      end

      expect_raises(PgORM::Error::RecordNotFound) do
        comp2.reload!
      end
    end

    it "works with where clauses" do
      comp1 = CompositeKeys.create!(key_one: "one", key_two: "two", payload: "test1")
      comp2 = CompositeKeys.create!(key_one: "one", key_two: "three", payload: "test2")
      comp3 = CompositeKeys.create!(key_one: "two", key_two: "two", payload: "test3")

      results = CompositeKeys.where(key_one: "one").to_a
      results.size.should eq(2)
      results.all? { |r| r.key_one == "one" }.should be_true
    end

    it "works with order clauses" do
      comp1 = CompositeKeys.create!(key_one: "c", key_two: "z", payload: "test1")
      comp2 = CompositeKeys.create!(key_one: "a", key_two: "y", payload: "test2")
      comp3 = CompositeKeys.create!(key_one: "b", key_two: "x", payload: "test3")

      results = CompositeKeys.order(:key_one).to_a
      results.size.should eq(3)
      results[0].key_one.should eq("a")
      results[1].key_one.should eq("b")
      results[2].key_one.should eq("c")
    end

    it "works with limit and offset" do
      5.times do |i|
        CompositeKeys.create!(key_one: "key#{i}", key_two: "val#{i}", payload: "test#{i}")
      end

      results = CompositeKeys.limit(2).to_a
      results.size.should eq(2)

      offset_results = CompositeKeys.limit(2).offset(2).to_a
      offset_results.size.should eq(2)
      offset_results.map(&.key_one).should_not eq(results.map(&.key_one))
    end

    it "works with count" do
      3.times do |i|
        CompositeKeys.create!(key_one: "key#{i}", key_two: "val#{i}", payload: "test#{i}")
      end

      CompositeKeys.count.should eq(3)
      CompositeKeys.where(key_one: "key1").count.should eq(1)
    end

    it "handles find with non-existent composite key" do
      expect_raises(PgORM::Error::RecordNotFound) do
        CompositeKeys.find!({"nonexistent", "key"})
      end
    end

    it "handles find? with non-existent composite key" do
      result = CompositeKeys.find?({"nonexistent", "key"})
      result.should be_nil
    end
  end
end

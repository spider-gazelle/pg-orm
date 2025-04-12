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
  end
end

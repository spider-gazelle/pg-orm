describe PgORM::Table do
  it "test primary key" do
    {:key_one, :key_two}.should eq(CompositeKeys.primary_key)
  end

  it "test CRUD" do
    comp = CompositeKeys.new(key_one: "one", key_two: "two", payload: "payload")
    comp.save!

    other = CompositeKeys.find!({"one", "two"})
    other.key_one.should eq comp.key_one
    other.key_two.should eq comp.key_two
    other.payload.should eq comp.payload

    # other.destroy
  end
end

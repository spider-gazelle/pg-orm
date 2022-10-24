require "./spec_helper"

describe PgORM::Persistence do
  it "should save,reload all persisted fields" do
    model = BasicModel.new

    model.new_record?.should be_true
    model.destroyed?.should be_false
    model.persisted?.should be_false

    model.age = 34
    model.name = "bob"
    model.address = "somewhere"

    model.new_record?.should be_true
    model.destroyed?.should be_false
    model.persisted?.should be_false

    model.save.should be_true

    model.persisted?.should be_true
    model.new_record?.should be_false
    model.destroyed?.should be_false

    loaded_model = BasicModel.find(model.id)
    loaded_model.should eq model

    model.destroy
    model.new_record?.should be_false
    model.destroyed?.should be_true
    model.persisted?.should be_false
  end

  it "should update only updated columns" do
    model = BasicModel.new
    model.name = "bob"
    model.age = 34
    model.hash = {"hello" => "world"}
    model.save

    model.new_record?.should be_false
    model.destroyed?.should be_false
    model.persisted?.should be_true

    model.name.should eq "bob"
    model.age.should eq 34
    model.hash.should eq({"hello" => "world"})
    model.@address.should be_nil

    model.hash = {"world" => "hello"}
    model.save

    model.reload!
    model.hash.should eq({"world" => "hello"})

    model.destroy
    model.destroyed?.should be_true
  end

  it "should destroy created record" do
    model = BasicModel.new(age: 34, name: "bob", address: "somewhere")

    model.new_record?.should be_true
    model.destroyed?.should be_false
    model.persisted?.should be_false

    model.save.should be_true

    model.persisted?.should be_true
    model.new_record?.should be_false
    model.destroyed?.should be_false

    loaded_model = BasicModel.find(model.id)
    loaded_model.should eq model

    model.destroy
    model.new_record?.should be_false
    model.destroyed?.should be_true
    model.persisted?.should be_false

    BasicModel.exists?(model.id).should be_false
  end

  it "can reload previously created record" do
    model = BasicModel.new

    model.name = "bob"
    model.address = "somewhere"
    model.age = 34

    model.save.should be_true
    id = model.id
    model.name = "bill"
    model.changed?.should be_true

    model_copy = BasicModel.find!(model.id)
    model_copy.name = "bib"
    model_copy.save!

    model.reload!

    model.changed?.should be_false

    model.id.should eq id
    model.name.should eq "bib"

    model.destroy
  end

  it "should clear table" do
    BasicModel.clear

    name = "Wobbuffet"
    5.times do
      BasicModel.create(name: name)
    end

    models = BasicModel.all.to_a
    models.size.should eq 5
    models.all?(&.name.==(name)).should be_true

    BasicModel.clear
    BasicModel.count.should eq 0
  end

  it "saves a model with defaults" do
    model = ModelWithDefaults.new

    model.name.should eq "bob"
    model.age.should eq 23
    model.address.should be_nil

    model.new_record?.should be_true
    model.destroyed?.should be_false
    model.persisted?.should be_false

    model.save.should be_true

    model.new_record?.should be_false
    model.destroyed?.should be_false
    model.persisted?.should be_true
    loaded_model = ModelWithDefaults.find(model.id)
    loaded_model.id.should eq model.id

    model.destroy
    model.new_record?.should be_false
    model.destroyed?.should be_true
    model.persisted?.should be_false
  end

  it "should support dirty attributes" do
    base = BasicModel.new
    changed_attributes = base.changed_attributes

    base.name = "change"
    base.changed_attributes.size.should eq(changed_attributes.size + 1)

    base = BasicModel.new(name: "bob")
    base.changed_attributes.empty?.should be_false

    # A saved model should have no changes
    base = BasicModel.create(name: "joe")
    base.changed_attributes.empty?.should be_true
  end

  it "performs validations" do
    model = ModelWithValidations.new(name: "")

    model.valid?.should be_false

    # Test create
    result = model.save
    result.should be_false
    model.errors.size.should eq 2

    expect_raises(PgORM::Error::RecordInvalid, message: "ModelWithValidations has invalid fields. `name` is required, `age` must be greater than 20") do
      model.save!
    end

    model.errors.clear

    model.name = "bob"
    model.age = 23

    model.save.should be_true
    model.valid?.should be_true

    # Test update
    model.age = 5
    model.valid?.should be_false
    model.save.should be_false
    expect_raises(PgORM::Error::RecordInvalid, message: "ModelWithValidations has an invalid field. `age` must be greater than 20") do
      model.save!
    end
    model.destroy
  end

  it "persists only persisted attributes" do
    model = LittleBitPersistent.create!(name: "Johnny Johnny", age: 100, address: "TOP SECRET")

    loaded_model = LittleBitPersistent.find(model.id)
    loaded_model.should_not be_nil
    if loaded_model
      loaded_model.address.should be_nil
      loaded_model.should_not eq model
      loaded_model.persistent_attributes.should eq model.persistent_attributes
    end

    model.destroy
  end

  it "should save/load fields with converters" do
    time = Time.unix(rand(1000000))
    model = ConvertedFields.create!(name: "gremlin", time: time)
    loaded = ConvertedFields.find!(model.id)

    loaded.time.should eq model.time
  end
end

describe "callbacks" do
  it "execute callbacks" do
    model = ModelWithCallbacks.new(name: "bob")

    # Test initialize
    model.name.should eq "bob"
    model.age.should eq 10
    model.address.should be_nil

    model.new_record?.should be_true
    model.destroyed?.should be_false
    model.persisted?.should be_false

    # Test create
    model.save.should be_true

    model.name.should eq "bob"
    model.age.should eq 10
    model.address.should eq "23"

    # Test Update
    model.address = "other"
    model.address.should eq "other"
    model.save.should be_true

    model.name.should eq "bob"
    model.age.should eq 30
    model.address.should eq "23"

    # Test destroy
    model.destroy
    model.new_record?.should be_false
    model.destroyed?.should be_true
    model.persisted?.should be_false

    model.name.should eq "joe"
  end

  it "skips destroy callbacks on delete" do
    model = ModelWithCallbacks.new(name: "bob")

    # Test initialize
    model.name.should eq "bob"
    model.age.should eq 10
    model.address.should be_nil

    model.new_record?.should be_true
    model.destroyed?.should be_false
    model.persisted?.should be_false

    # Test create
    model.save.should be_true

    # Test delete
    model.delete
    model.new_record?.should be_false
    model.destroyed?.should be_true
    model.persisted?.should be_false

    model.name.should eq "bob"
  end

  it "skips callbacks when updating fields" do
    model = ModelWithCallbacks.new(name: "bob")

    # Test initialize
    model.name.should eq "bob"
    model.address.should be_nil
    model.age.should eq 10

    model.new_record?.should be_true
    model.destroyed?.should be_false
    model.persisted?.should be_false

    # Test create
    result = model.save
    result.should be_true

    model.name.should eq "bob"
    model.age.should eq 10
    model.address.should eq "23"

    # Test Update
    model.update_fields(address: "other")

    model.address.should eq "other"
    loaded = ModelWithCallbacks.find(model.id)
    loaded.address.should eq "other"

    # Test delete skipping callbacks
    model.delete
    model.new_record?.should be_false
    model.destroyed?.should be_true
    model.persisted?.should be_false

    model.name.should eq "bob"
  end

  it "test save" do
    group = Group.new(name: "A")
    group.description = "D"

    group.save
    group.changed?.should be_false
    group.id?.should_not eq(nil)

    group.name = "Testing"
    group.description = "Description"
    group.save
    group.changed?.should be_false

    group = Group.find(group.id)
    group.name.should eq("Testing")
    group.description.should eq("Description")
  end

  it "test new_record" do
    group = Group.new(name: "A")
    group.new_record?.should be_true

    group.save
    group.new_record?.should_not be_true

    group = Group.find(group.id)
    group.new_record?.should_not be_true

    Group.all.none?(&.new_record?).should be_true
  end

  it "test persisted?" do
    group = Group.new(name: "A")
    group.persisted?.should_not be_true

    group.save
    group.persisted?.should be_true

    group = Group.find(group.id)
    group.persisted?.should be_true

    Group.all.all?(&.persisted?).should be_true
  end

  it "test class update" do
    group = Group.create(name: "A")
    Group.update(group.id, name: "B")
    group.name.should eq("A")
    group.reload!.name.should eq("B")
  end

  it "test update" do
    group = Group.create(name: "A")

    group.update(name: "B", description: "some description")
    group.name.should eq("B")
    group.description.should eq("some description")
    group.changed?.should be_false

    group = Group.find(group.id)
    group.name.should eq("B")
    group.description.should eq("some description")
  end

  it "test class delete" do
    ids = 5.times.map { |i| Group.create(name: i.to_s).id }.to_a
    Group.delete(ids[0])
    Group.delete(ids[2], ids[3])

    Group.exists?(ids[0]).should be_false
    Group.exists?(ids[1]).should be_true
    Group.exists?(ids[2]).should be_false
    Group.exists?(ids[3]).should be_false
    Group.exists?(ids[4]).should be_true
  end

  it "test delete" do
    group = Group.create(name: "A")
    group.delete
    group.destroyed?.should be_true
    group.persisted?.should be_false
    group.new_record?.should be_false
    Group.find?(group.id).should be_nil
  end

  it "test reload" do
    group = Group.create(name: "test")
    Group.find(group.id).update(name: "reloaded")

    group.reload!.name.should eq("reloaded")
    group.name.should eq("reloaded")
    group.new_record?.should be_false
    group.persisted?.should be_true
    group.changed?.should be_false

    Group.find(group.id).delete
    expect_raises(PgORM::Error::RecordNotFound) do
      group.reload!
    end
  end
end

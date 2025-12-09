require "./spec_helper"

describe "Enum Fields" do
  it "can create enum fields" do
    model = EnumFields.create!
    model.status.should be_nil
    model.role.should eq(EnumFields::Role::Issue)
    model.permissions.should eq(EnumFields::Permissions::Read | EnumFields::Permissions::Write)
    model.active.should be_false
  end

  it "can be updated" do
    model = EnumFields.create!(status: EnumFields::Status::Opened)
    model.status.should eq(EnumFields::Status::Opened)
    model.status = EnumFields::Status::Closed
    model.active = true
    model.save!
    model.status.should eq(EnumFields::Status::Closed)
    model.active.should be_true
  end

  it "can query enum" do
    model = EnumFields.create!(status: EnumFields::Status::Closed)
    model.status = EnumFields::Status::Opened

    old = EnumFields.find(model.id)
    old.status.should eq(EnumFields::Status::Closed)
  end

  it "can query with where clause on enum" do
    EnumFields.clear
    model1 = EnumFields.create!(status: EnumFields::Status::Opened)
    model2 = EnumFields.create!(status: EnumFields::Status::Closed)
    model3 = EnumFields.create!(status: EnumFields::Status::Opened)

    results = EnumFields.where({:status => EnumFields::Status::Opened.value}).to_a
    results.size.should eq(2)
    results.all? { |r| r.status == EnumFields::Status::Opened }.should be_true
  end

  it "can order by enum" do
    EnumFields.clear
    model1 = EnumFields.create!(role: EnumFields::Role::Issue)
    model2 = EnumFields.create!(role: EnumFields::Role::Bug)
    model3 = EnumFields.create!(role: EnumFields::Role::Critical)

    results = EnumFields.order(:role).to_a
    results.size.should eq(3)
    # Should be ordered by enum value (1, 2, 3)
    results[0].role.should eq(EnumFields::Role::Issue)
    results[1].role.should eq(EnumFields::Role::Bug)
    results[2].role.should eq(EnumFields::Role::Critical)
  end

  it "handles nil enum values" do
    model = EnumFields.create!(status: nil)
    model.status.should be_nil

    found = EnumFields.find(model.id)
    found.status.should be_nil
  end

  it "can update enum to nil" do
    model = EnumFields.create!(status: EnumFields::Status::Opened)
    model.status.should eq(EnumFields::Status::Opened)

    model.status = nil
    model.save!

    found = EnumFields.find(model.id)
    found.status.should be_nil
  end

  it "handles flag enums correctly" do
    model = EnumFields.create!(
      permissions: EnumFields::Permissions::Read | EnumFields::Permissions::Write
    )

    model.permissions.should eq(EnumFields::Permissions::Read | EnumFields::Permissions::Write)
    model.permissions.includes?(EnumFields::Permissions::Read).should be_true
    model.permissions.includes?(EnumFields::Permissions::Write).should be_true
  end

  it "can query multiple enum values" do
    EnumFields.clear
    model1 = EnumFields.create!(status: EnumFields::Status::Opened)
    model2 = EnumFields.create!(status: EnumFields::Status::Closed)
    model3 = EnumFields.create!(status: nil)

    results = EnumFields.where({:status => [EnumFields::Status::Opened.value, EnumFields::Status::Closed.value]}).to_a
    results.size.should eq(2)
  end

  it "handles boolean enum correctly" do
    model = EnumFields.create!(active: true)
    model.active.should be_true

    found = EnumFields.find(model.id)
    found.active.should be_true

    model.active = false
    model.save!

    found = EnumFields.find(model.id)
    found.active.should be_false
  end
end

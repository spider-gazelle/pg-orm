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
end

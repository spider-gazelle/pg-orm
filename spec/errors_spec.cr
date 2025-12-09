require "./spec_helper"

class PgORM::Error
  describe RecordInvalid do
    it "#errors" do
      m = ModelWithValidations.new(name: "")
      m.valid?.should be_false
      e = RecordInvalid.new(m)
      e.errors.should eq [{field: :name, message: "is required"}, {field: :age, message: "must be greater than 20"}]
    end

    it "#to_s" do
      m = ModelWithValidations.new(name: "")
      m.valid?.should be_false
      e = RecordInvalid.new(m)
      e.to_s.should eq "ModelWithValidations has invalid fields. `name` is required, `age` must be greater than 20"
    end
  end

  describe RecordNotFound do
    it "can be raised" do
      expect_raises(RecordNotFound) do
        raise RecordNotFound.new
      end
    end

    it "raises with custom message" do
      expect_raises(RecordNotFound, "User with id 123 not found") do
        raise RecordNotFound.new("User with id 123 not found")
      end
    end

    it "is raised by find!" do
      expect_raises(RecordNotFound) do
        BasicModel.find!(99999)
      end
    end

    it "is raised by find_by" do
      expect_raises(RecordNotFound) do
        BasicModel.find_by(name: "nonexistent")
      end
    end
  end

  describe RecordNotSaved do
    it "can be raised" do
      expect_raises(RecordNotSaved) do
        raise RecordNotSaved.new
      end
    end

    it "raises with custom message" do
      expect_raises(RecordNotSaved, "Failed to save user") do
        raise RecordNotSaved.new("Failed to save user")
      end
    end
  end

  describe Error do
    it "is base class for all PgORM errors" do
      RecordNotFound.new.should be_a(Error)
      RecordNotSaved.new.should be_a(Error)
      RecordInvalid.new(ModelWithValidations.new).should be_a(Error)
    end
  end
end

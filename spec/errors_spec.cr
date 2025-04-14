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
end

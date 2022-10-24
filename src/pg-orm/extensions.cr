# These extensions are provided for JSON/YAML use only. As reading/writing to database doesn't require these converters

# :nodoc:
module Time::EpochConverter
  def self.from_rs(rs : DB::ResultSet)
    rs.read(Time)
  end
end

module Time::EpochMillisConverter
  def self.from_rs(rs : DB::ResultSet)
    rs.read(Time)
  end
end

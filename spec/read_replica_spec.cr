require "./spec_helper"

# These specs exercise read/write connection routing. There is only one physical
# database in CI, so the "replica" URL points at the same database but tags its
# connections with a distinct `application_name`. Querying
# `current_setting('application_name')` then reveals which pool served a query.
describe "read replica routing" do
  # A replica URL pointing at the same test DB, but identifiable via app name.
  read_url = begin
    uri = URI.parse(ENV["PG_DATABASE_URL"])
    params = URI::Params.parse(uri.query || "")
    params["application_name"] = "pgorm_replica"
    uri.query = params.to_s
    uri.to_s
  end

  # Returns the application_name reported by the connection a query is routed to.
  served_by = ->(read : Bool) do
    PgORM::Database.connection(read: read) do |db|
      db.scalar("SELECT current_setting('application_name')").as(String)
    end
  end

  # Always leave replica routing disabled for the rest of the suite.
  after_each { PgORM::Database.parse_read(nil) }

  it "uses the primary when no replica is configured" do
    PgORM::Database.parse_read(nil)
    PgORM::Database.read_pool?.should be_nil
    served_by.call(true).should_not eq("pgorm_replica")
  end

  it "opens a distinct pool when a replica is configured" do
    PgORM::Database.parse_read(read_url)
    pool = PgORM::Database.read_pool?
    pool.should_not be_nil
    pool.should_not eq(PgORM::Database.pool)
  end

  it "routes standalone reads to the replica" do
    PgORM::Database.parse_read(read_url)
    served_by.call(true).should eq("pgorm_replica")
  end

  it "routes writes to the primary" do
    PgORM::Database.parse_read(read_url)
    served_by.call(false).should_not eq("pgorm_replica")
  end

  it "pins reads to the primary inside a transaction" do
    PgORM::Database.parse_read(read_url)
    PgORM::Database.transaction do
      # An explicit read inside a transaction must reuse the pinned primary
      # connection so it observes uncommitted writes (read-your-writes).
      served_by.call(true).should_not eq("pgorm_replica")
    end
  end

  it "pins reads to the primary inside with_connection" do
    PgORM::Database.parse_read(read_url)
    PgORM::Database.with_connection do
      served_by.call(true).should_not eq("pgorm_replica")
    end
  end

  it "routes model SELECTs through the replica without breaking persistence" do
    PgORM::Database.parse_read(read_url)
    # Write goes to primary, subsequent read is routed to the replica pool.
    # Since both point at the same physical DB, the row is visible end-to-end.
    group = Group.create(name: "replica-routing")
    Group.exists?(group.id).should be_true
    Group.find(group.id).name.should eq("replica-routing")
  end

  it "disables replica routing when reset to nil" do
    PgORM::Database.parse_read(read_url)
    PgORM::Database.read_pool?.should_not be_nil

    PgORM::Database.parse_read(nil)
    PgORM::Database.read_pool?.should be_nil
    served_by.call(true).should_not eq("pgorm_replica")
  end
end

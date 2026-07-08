require "./spec_helper"

# Regression coverage for the "poisoned connection" bug: when a transaction
# fails at COMMIT (or ROLLBACK), crystal-db has already issued the SQL but has
# not cleared the connection's transaction flag. If that connection is returned
# to the pool it stays flagged "in transaction", and the next checkout raises
# `There is an existing transaction in this connection`.
#
# A `UNIQUE ... DEFERRABLE INITIALLY DEFERRED` constraint lets us trigger a
# COMMIT-time failure deterministically — the uniqueness check is deferred to
# COMMIT, so inserting a duplicate inside the transaction makes `COMMIT` raise,
# exactly like a serialization failure (40001) deferred to commit under
# SERIALIZABLE isolation does in production.
describe PgORM::Database do
  describe "transaction finalization failures" do
    before_all do
      SpecConnection.exec_sql("DROP TABLE IF EXISTS deferred_uniques")
      SpecConnection.exec_sql(<<-SQL)
        CREATE TABLE deferred_uniques (
          id SERIAL PRIMARY KEY,
          val INT NOT NULL,
          CONSTRAINT deferred_uniques_val_key UNIQUE (val) DEFERRABLE INITIALLY DEFERRED
        )
        SQL
    end

    after_all do
      SpecConnection.exec_sql("DROP TABLE IF EXISTS deferred_uniques")
    end

    before_each do
      SpecConnection.exec_sql("TRUNCATE deferred_uniques")
    end

    it "discards the connection when COMMIT fails" do
      captured : DB::Connection? = nil

      expect_raises(PQ::PQError) do
        PgORM::Database.transaction do |tx|
          captured = tx.connection
          tx.connection.exec("INSERT INTO deferred_uniques (val) VALUES (1)")
          # duplicate — the deferred UNIQUE check fires at COMMIT, not here
          tx.connection.exec("INSERT INTO deferred_uniques (val) VALUES (1)")
        end
      end

      # The connection must not be handed back to the pool still flagged
      # "in transaction"; discarding it closes the connection.
      conn = captured.not_nil!
      conn.closed?.should be_true
    end

    it "keeps the pool healthy after a COMMIT failure" do
      expect_raises(PQ::PQError) do
        PgORM::Database.transaction do |tx|
          tx.connection.exec("INSERT INTO deferred_uniques (val) VALUES (1)")
          tx.connection.exec("INSERT INTO deferred_uniques (val) VALUES (1)")
        end
      end

      # End-to-end guard: the pool must stay usable after a commit failure.
      # (Whether a *poisoned* connection is actually reused depends on pool
      # scheduling — the deterministic invariant that prevents the user-facing
      # "existing transaction in this connection" 500 is asserted above: the
      # connection is always discarded, so it can never be handed back.)
      PgORM::Database.connection do |db|
        db.query_one("SELECT 1", as: Int32).should eq(1)
      end

      # A follow-up transaction must also succeed and actually commit.
      PgORM::Database.transaction do |tx|
        tx.connection.exec("INSERT INTO deferred_uniques (val) VALUES (2)")
      end

      SpecConnection.connection do |db|
        db.query_one("SELECT COUNT(*) FROM deferred_uniques WHERE val = 2", as: Int64).should eq(1_i64)
      end
    end

    it "does not discard the connection on an ordinary rollback" do
      captured : DB::Connection? = nil

      expect_raises(Exception, "boom") do
        PgORM::Database.transaction do |tx|
          captured = tx.connection
          tx.connection.exec("INSERT INTO deferred_uniques (val) VALUES (3)")
          raise "boom"
        end
      end

      # A clean rollback leaves the connection reusable — it must not be closed.
      captured.not_nil!.closed?.should be_false

      # And nothing was persisted.
      SpecConnection.connection do |db|
        db.query_one("SELECT COUNT(*) FROM deferred_uniques WHERE val = 3", as: Int64).should eq(0_i64)
      end

      # The pool is still usable.
      PgORM::Database.connection do |db|
        db.query_one("SELECT 1", as: Int32).should eq(1)
      end
    end
  end
end

require "./spec_helper"

# PPT-2642. `release` is the single point where a connection goes back to the
# pool, and it only rolls anything back when it finds this fiber's transaction
# in `@@transactions`. A connection whose transaction is open but *not* recorded
# there is therefore handed straight back to the pool with its `BEGIN` still
# live.
#
# Once that happens the connection is unusable for writes for the life of the
# process — `DB::Connection#begin_transaction` raises before it issues anything,
# because crystal-db sets the connection's transaction flag and only clears it
# in `do_close`, which a failed COMMIT never reaches. Reads keep working, since
# they simply run inside the orphaned transaction.
describe PgORM::Database do
  describe "returning connections to the pool" do
    it "does not release a connection that is still inside a transaction" do
      conn = PgORM::Database.checkout
      # A transaction the module does not know about — the same state a failed
      # COMMIT leaves behind, reached here without needing the race.
      conn.begin_transaction
      PgORM::Database.release

      # The pool must not hand that connection to anyone in this state.
      5.times do
        PgORM::Database.transaction do |tx|
          tx.connection.scalar("SELECT 1").should eq 1
        end
      end
    end
  end
end

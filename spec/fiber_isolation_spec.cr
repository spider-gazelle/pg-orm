require "./spec_helper"

# A fiber's pinned connection and open transaction belong to that fiber alone.
# They used to live in class-level hashes keyed by `Fiber#object_id`, which
# fibers running on other threads of a parallel execution context mutate
# concurrently, losing entries: a fiber then finds no record of its own open
# transaction and raises "There is an existing transaction in this connection".
describe PgORM::Database do
  describe "fiber isolation" do
    it "does not share a pinned connection with a spawned fiber" do
      PgORM::Database.transaction do |tx|
        inner = Channel(DB::Connection).new
        spawn do
          PgORM::Database.with_connection { |conn| inner.send conn }
        end
        inner.receive.same?(tx.connection).should be_false
      end
    end

    {% if Fiber.has_constant?(:ExecutionContext) %}
      it "keeps each fiber's connection and transaction separate across threads" do
        context = Fiber::ExecutionContext::Parallel.new("pg-orm-spec", 8)
        fibers = 32
        done = Channel(Exception?).new(fibers)

        fibers.times do
          context.spawn do
            error = nil
            begin
              50.times do
                PgORM::Database.transaction do |tx|
                  PgORM::Database.transaction do |nested|
                    nested.connection.same?(tx.connection).should be_true
                    nested.connection.scalar("SELECT 1").should eq 1
                  end
                  PgORM::Database.with_connection(&.same?(tx.connection).should(be_true))
                end
              end
            rescue ex
              error = ex
            end
            done.send error
          end
        end

        errors = Array.new(fibers) { done.receive }.compact
        errors.map { |ex| "#{ex.class}: #{ex.message}" }.uniq.should be_empty
      end
    {% end %}
  end
end

require "./spec_helper"
alias Query = PgORM::Query

describe PgORM::Query do
  describe "Query" do
    it "returns records matching passed ids" do
      correct_records = Array.new(size: 5) do |i|
        BasicModel.create!(name: "Fake Name #{i}", age: 10)
      end
      ids = (correct_records.compact_map &.id).sort
      found_ids = (BasicModel.find_all(ids).to_a.compact_map &.id).sort
      found_ids.should eq ids
    end

    it "query array column" do
      tree1 = Tree.new
      tree2 = Tree.new

      roots = Array(String).new(3) do
        Root.create!(length: (rand * 10).to_f64).id.to_s
      end

      # Check all roots created
      roots.none?(Nil).should be_true

      tree1.roots = roots[0..1]
      tree2.roots = roots[1..2]

      tree1.save
      tree2.save

      tree_ids = [tree1.id, tree2.id].compact

      get_tree_ids = ->(root_id : String) {
        # Refer to ./spec_models for `Tree#by_root_id` query
        Tree.by_root_id(root_id).compact_map(&.id).sort!
      }

      # Check the correct
      get_tree_ids.call(roots.first).should eq [tree_ids.first]
      get_tree_ids.call(roots[1]).should eq tree_ids.sort
      get_tree_ids.call(roots[2]).should eq [tree_ids[1]]
    end

    it "query array column using where" do
      tree1 = Tree.new
      tree2 = Tree.new

      roots = Array(String).new(3) do
        Root.create!(length: (rand * 10).to_f64).id.to_s
      end

      # Check all roots created
      roots.none?(Nil).should be_true

      tree1.roots = roots[0..1]
      tree2.roots = roots[1..2]

      tree1.save
      tree2.save

      tree_ids = [tree1.id, tree2.id].compact

      get_tree_ids = ->(root_id : String) {
        # Refer to ./spec_models for `Tree#by_root_id_where` query
        Tree.by_root_id_where(root_id).compact_map(&.id).sort!
      }

      # Check the correct
      get_tree_ids.call(roots.first).should eq [tree_ids.first]
      get_tree_ids.call(roots[1]).should eq tree_ids.sort
      get_tree_ids.call(roots[2]).should eq [tree_ids[1]]
    end

    it "test each iterator" do
      users = User.all
      iter = users.each

      typeof(iter.next).should eq(Iterator::Stop | User)
      until (value = iter.next).is_a?(Iterator::Stop)
        value.is_a?(User).should be_true
      end

      users.each { |rec| rec.is_a?(User).should be_true }
    end

    it "test each iterator with cache" do
      users = User.all
      users.to_a

      iter = users.each

      typeof(iter.next).should eq(Iterator::Stop | User)
      until (value = iter.next).is_a?(Iterator::Stop)
        value.is_a?(User).should be_true
      end
    end

    it "test all" do
      User.all.is_a?(PgORM::Collection(User)).should be_true
      Group.all.is_a?(PgORM::Collection(Group)).should be_true

      User.all.to_a.is_a?(Array(User)).should be_true
      Group.all.to_a.is_a?(Array(Group)).should be_true
    end

    it "test none" do
      typeof(User.none).should eq(PgORM::Collection(User))
      User.none.to_a.should eq([] of User)

      typeof(Group.none).should eq(PgORM::Collection(Group))
      Group.none.to_a.should eq([] of Group)

      typeof(User.select("*").none).should eq(PgORM::Collection(User))
      User.select("*").none.to_a.should eq([] of User)

      typeof(Group.select("*").none).should eq(PgORM::Collection(Group))
      Group.select("*").none.to_a.should eq([] of Group)

      Group.none.find?(group_id).should be_nil
      Group.none.find_by?(id: group_id).should be_nil
      Group.none.exists?(group_id).should be_false

      Group.none.count.should eq(0)
      Group.none.sum("LENGTH(name)").should eq(0)
      Group.none.average("LENGTH(name)").should eq(0)
      Group.none.minimum("name").should be_nil
      Group.none.maximum("name").should be_nil
    end

    it "test ids" do
      typeof(User.ids).should eq(Array(UUID))
      typeof(Group.ids).should eq(Array(Int32))

      User.ids.is_a?(Array(UUID)).should be_true
      Group.ids.is_a?(Array(Int32)).should be_true
    end

    it "test find" do
      typeof(User.find(UUID.random)).should eq(User)
      typeof(Group.find(group_id)).should eq(Group)

      expect_raises(PgORM::Error::RecordNotFound) { User.find(UUID.random) }
      Group.find(group_id).is_a?(Group).should be_true
    end

    it "test find?" do
      typeof(User.find?(UUID.random)).should eq(User?)
      typeof(Group.find?(group_id)).should eq(Group?)

      User.find?(UUID.random).should be_nil
      Group.find?(group_id).is_a?(Group).should be_true
    end

    it "test take" do
      User.take.is_a?(User).should be_true
      Group.take.is_a?(Group).should be_true
      expect_raises(PgORM::Error::RecordNotFound) { User.where(uuid: UUID.random).take }
    end

    it "test take?" do
      User.take?.is_a?(User).should be_true
      Group.take?.is_a?(Group).should be_true
      User.where(uuid: UUID.random).take?.should be_nil
    end

    it "test where(NamedTuple)" do
      group = Group.first
      User.where({group_id: group.id}).to_a.size.should be > 1
    end

    it "test where_not(NamedTuple)" do
      group = Group.first
      User.where_not({group_id: group.id}).to_a.size.should be > 1
    end

    it "test find_by" do
      typeof(User.find_by(uuid: UUID.random)).should eq(User)
      typeof(Group.find_by(id: group_id)).should eq(Group)

      expect_raises(PgORM::Error::RecordNotFound) { User.find_by(uuid: UUID.random) }
      Group.find_by(id: group_id).is_a?(Group).should be_true
    end

    it "test find_by?" do
      typeof(User.find_by?(uuid: UUID.random)).should eq(User?)
      typeof(Group.find_by?(id: group_id)).should eq(Group?)

      User.find_by?(uuid: UUID.random).should be_nil
      Group.find_by?(id: group_id).is_a?(Group).should be_true
    end

    it "test find_all_by_sql" do
      typeof(User.find_all_by_sql("SELECT * FROM users")).should eq(Array(User))
      typeof(Group.find_all_by_sql("SELECT * FROM groups")).should eq(Array(Group))

      User.find_all_by_sql("SELECT * FROM users").is_a?(Array(User)).should be_true
      Group.find_all_by_sql("SELECT * FROM groups").is_a?(Array(Group)).should be_true
    end

    it "test find_one_by_sql" do
      user_sql = "SELECT * FROM users WHERE uuid = $1 LIMIT 1"
      group_sql = "SELECT * FROM groups WHERE id = $1 LIMIT 1"

      typeof(User.find_one_by_sql("SELECT * FROM users LIMIT 1")).should eq(User)
      typeof(Group.find_one_by_sql("SELECT * FROM groups LIMIT 1")).should eq(Group)

      user = User.find_one_by_sql("SELECT * FROM users LIMIT 1")
      user.is_a?(User).should be_true
      Group.find_one_by_sql("SELECT * FROM groups LIMIT 1").is_a?(Group).should be_true

      User.find_all([user.id]).size.should eq 1

      expect_raises(PgORM::Error::RecordNotFound) { User.find_one_by_sql(user_sql, UUID.random) }
      expect_raises(PgORM::Error::RecordNotFound) { Group.find_one_by_sql(group_sql, Int32::MAX - 10) }
    end

    it "test find_one_by_sql?" do
      user_sql = "SELECT * FROM users WHERE uuid = $1 LIMIT 1"
      group_sql = "SELECT * FROM groups WHERE id = $1 LIMIT 1"

      typeof(User.find_one_by_sql?("SELECT * FROM users LIMIT 1")).should eq(User?)
      typeof(Group.find_one_by_sql?("SELECT * FROM groups LIMIT 1")).should eq(Group?)

      User.find_one_by_sql?("SELECT * FROM users LIMIT 1").is_a?(User).should be_true
      Group.find_one_by_sql?("SELECT * FROM groups LIMIT 1").is_a?(Group).should be_true

      User.find_one_by_sql?(user_sql, UUID.random).should be_nil
      Group.find_one_by_sql?(group_sql, Int32::MAX).should be_nil
    end

    it "test first" do
      typeof(Group.first).should eq(Group)
      typeof(User.where(group_id: group_id).order(:name).first).should eq(User)

      Group.first.is_a?(Group).should be_true
      User.where(group_id: group_id).order(:name).first.is_a?(User).should be_true
    end

    it "test first?" do
      typeof(Group.first?).should eq(Group?)
      typeof(User.where(group_id: group_id).order(:name).first?).should eq(User?)

      Group.first?.is_a?(Group).should be_true
      User.where(group_id: group_id).order(:name).first?.is_a?(User).should be_true
    end

    it "test last" do
      typeof(Group.last).should eq(Group)
      typeof(User.where(group_id: group_id).order(:name).last).should eq(User)

      Group.last.is_a?(Group).should be_true
      User.where(group_id: group_id).order(:name).last.is_a?(User).should be_true
    end

    it "test last?" do
      typeof(Group.last?).should eq(Group?)
      typeof(User.where(group_id: group_id).order(:name).last?).should eq(User?)

      Group.last?.is_a?(Group).should be_true
      User.where(group_id: group_id).order(:name).last?.is_a?(User).should be_true
      User.where(group_id: group_id).order("name DESC, group_id ASC").last.is_a?(User).should be_true
    end

    it "test exists?" do
      User.where(group_id: group_id).exists?.should be_true
      Group.exists?(group_id).should be_true
    end

    it "test where" do
      users = User.where("name LIKE ?", "X-%").where("group_id BETWEEN ? AND ?", -1, 200)
      users.to_a.is_a?(Array(User)).should be_true
    end

    it "test where regex" do
      users = User.where(group_id: group_id)

      2.should eq users.where(name: /X-/).count
      0.should eq users.where(name: /x-/).count
      2.should_not eq users.where_not(name: /X-/).count

      2.should eq users.where(name: /x-/i).count
      0.should eq users.where_not(name: /x-/i).count
    end

    it "test order" do
      users = User.order(:name, "group_id DESC")
      users.to_a.is_a?(Array(User)).should be_true
    end

    it "test pluck" do
      User.pluck(:uuid).each(&.is_a?(UUID | String))
      User.pluck("LENGTH(name)").each(&.is_a?(Int))
    end

    it "test count" do
      total = User.count
      total.is_a?(Int64).should be_true

      d1 = User.distinct.count(:group_id)
      d1.is_a?(Int64).should be_true

      d2 = User.count(:group_id, distinct: true)
      d2.is_a?(Int64).should be_true

      total.should eq(User.count(:group_id))
      d1.should be < total
      d1.should eq(d2)
    end

    it "test average" do
      User.average(:group_id).is_a?(Float64).should be_true
      User.average("LENGTH(name)").is_a?(Float64).should be_true
    end

    it "test sum" do
      User.sum(:group_id).is_a?(Int64).should be_true
      User.sum("LENGTH(name)").is_a?(Int64).should be_true
    end

    it "test minimum" do
      User.minimum(:group_id).is_a?(Int).should be_true
      User.minimum(:name).is_a?(String).should be_true
      User.minimum("LENGTH(name)").is_a?(Int).should be_true
    end

    it "test maximum" do
      User.maximum(:group_id).is_a?(Int).should be_true
      User.maximum(:name).is_a?(String).should be_true
      User.maximum("LENGTH(name)").is_a?(Int).should be_true
    end
  end

  describe "QueryBuilder" do
    it "test select" do
      b1 = Query::Builder.new("foos")
      b2 = b1.select(:id).select(:name, "1 AS one")

      b1.selects.should be_nil
      b2.selects.should eq([:id, :name, "1 AS one"])
    end

    it "test select!" do
      b = Query::Builder.new("foos")
      b.select!(:id).select!(:name, "1 AS one")

      b.selects.should eq([:id, :name, "1 AS one"])
    end

    it "test distinct" do
      b1 = Query::Builder.new("foos")
      b2 = b1.distinct

      b1.distinct?.should be_false
      b2.distinct?.should be_true
    end

    it "test distinct!" do
      b = Query::Builder.new("foos")
      b.distinct?.should be_false

      b.distinct!
      b.distinct?.should be_true
    end

    it "test limit" do
      b1 = Query::Builder.new("foos")
      b2 = b1.limit(50)

      b1.limit?.should be_nil
      b2.limit?.should eq(50)
    end

    it "test limit!" do
      b = Query::Builder.new("foos")
      b.limit!(50)

      b.limit?.should eq(50)
    end

    it "test offset" do
      b1 = Query::Builder.new("foos")
      b2 = b1.offset(200)

      b1.offset?.should be_nil
      b2.offset?.should eq(200)
    end

    it "test offset!" do
      b = Query::Builder.new("foos")
      b.offset!(200)

      b.offset?.should eq(200)
    end

    it "test where" do
      b1 = Query::Builder.new("foos")

      b2 = b1.where({:id => 1})
      b3 = b2.where({name: "something"})

      uuid = UUID.random
      b4 = b2.where(group_id: uuid, minimum: 123.456)

      b5 = b2.where("key LIKE ?", "test%")
        .where("value > ? AND value < ?", 10, 20)

      b1.conditions.should be_nil
      b2.conditions.should eq([Query::Builder::Condition.new(:id, 1)])
      b3.conditions.should eq([Query::Builder::Condition.new(:id, 1), Query::Builder::Condition.new(:name, "something")])
      b4.conditions.should eq([Query::Builder::Condition.new(:id, 1),
                               Query::Builder::Condition.new(:group_id, uuid),
                               Query::Builder::Condition.new(:minimum, 123.456)])

      b5.conditions.should eq([Query::Builder::Condition.new(:id, 1),
                               Query::Builder::RawCondition.new("key LIKE ?", ["test%"] of PgORM::Value),
                               Query::Builder::RawCondition.new("value > ? AND value < ?", [10, 20] of PgORM::Value)])
    end

    it "test where in" do
      b1 = Query::Builder.new("foos")
      b2 = b1.where(id: [1, 3, 4])

      b1.conditions.should be_nil
      b2.conditions.should eq([Query::Builder::Condition.new(:id, [1, 3, 4] of PgORM::Value)])
    end

    it "test where not" do
      b1 = Query::Builder.new("foos")
      b2 = b1.where_not(id: 12)
      b3 = b2.where_not("id > ?", 12345).where("name IS NOT NULL")

      b1.conditions.should be_nil

      b2.conditions.should eq([
        Query::Builder::Condition.new(:id, 12, not: true),
      ])

      b3.conditions.should eq([
        Query::Builder::Condition.new(:id, 12, not: true),
        Query::Builder::RawCondition.new("id > ?", [12345] of PgORM::Value, not: true),
        Query::Builder::RawCondition.new("name IS NOT NULL", nil),
      ])
    end

    it "test where not!" do
      b = Query::Builder.new("foos")
      b.where_not!(id: 12)
      b.where_not!("id > ?", 12345)
      b.where!("name IS NOT NULL")

      b.conditions.should eq([
        Query::Builder::Condition.new(:id, 12, not: true),
        Query::Builder::RawCondition.new("id > ?", [12345] of PgORM::Value, not: true),
        Query::Builder::RawCondition.new("name IS NOT NULL", nil),
      ])
    end

    it "test where!" do
      b = Query::Builder.new("foos")
      b.where!({:id => 1})
        .where!({name: "something"})
        .where!(minimum: 123.4)
        .where!("key LIKE ?", "test%")
        .where!("value > ? AND value < ?", 10, 20)
      b.conditions.should eq([
        Query::Builder::Condition.new(:id, 1),
        Query::Builder::Condition.new(:name, "something"),
        Query::Builder::Condition.new(:minimum, 123.4),
        Query::Builder::RawCondition.new("key LIKE ?", ["test%"] of PgORM::Value),
        Query::Builder::RawCondition.new("value > ? AND value < ?", [10, 20] of PgORM::Value),
      ])
    end

    it "test order" do
      b1 = Query::Builder.new("foos")
      b2 = b1.order(:id)
      b3 = b2.order(:name, :value)
        .order(minimum: :desc)
        .order({:maximum => :asc})

      b1.orders.should be_nil
      b2.orders.should eq([{:id, :asc}])
      b3.orders.should eq([
        {:id, :asc},
        {:name, :asc},
        {:value, :asc},
        {:minimum, :desc},
        {:maximum, :asc},
      ])
    end

    it "test order!" do
      b = Query::Builder.new("foos")
      b.order!(:id)
        .order!(:name, :value)
        .order!(minimum: :desc)
        .order!({:maximum => :asc})

      b.orders.should eq([
        {:id, :asc},
        {:name, :asc},
        {:value, :asc},
        {:minimum, :desc},
        {:maximum, :asc},
      ])
    end

    it "test reorder" do
      b1 = Query::Builder.new("foos").order(:id, :name)
      b2 = b1.reorder(:value, :minimum)
      b3 = b2.reorder(:id)
      b4 = b3.reorder(id: :desc)
      b5 = b3.reorder({:id => :desc})

      b2.orders.should eq([{:value, :asc}, {:minimum, :asc}])
      b3.orders.should eq([{:id, :asc}])
      b4.orders.should eq([{:id, :desc}])
      b5.orders.should eq([{:id, :desc}])
    end

    it "test reorder!" do
      b = Query::Builder.new("foos").order(:id, :name)

      b.reorder!(:value, :minimum)
      b.orders.should eq([{:value, :asc}, {:minimum, :asc}])

      b.reorder!(:id)
      b.orders.should eq([{:id, :asc}])

      b.reorder!(id: :desc)
      b.orders.should eq([{:id, :desc}])

      b.reorder!({:value => :asc})
      b.orders.should eq([{:value, :asc}])
    end

    it "test unscope" do
      b = Query::Builder.new("foos")
        .select(:id, :group_id)
        .where(group_id: 1)
        .order(:id)
        .limit(10)
        .offset(200)

      b1 = b.unscope(:select)
      b2 = b.unscope(:where)
      b3 = b.unscope(:order)
      b4 = b.unscope(:limit)
      b5 = b.unscope(:offset)

      b.selects?.should_not be_nil
      b.conditions?.should_not be_nil
      b.orders?.should_not be_nil
      b.limit?.should_not be_nil
      b.offset?.should_not be_nil

      b1.selects?.should be_nil
      b1.conditions?.should_not be_nil
      b1.orders?.should_not be_nil
      b1.limit?.should_not be_nil
      b1.offset?.should_not be_nil

      b2.selects?.should_not be_nil
      b2.conditions?.should be_nil
      b2.orders?.should_not be_nil
      b2.limit?.should_not be_nil
      b2.offset?.should_not be_nil

      b3.selects?.should_not be_nil
      b3.conditions?.should_not be_nil
      b3.orders?.should be_nil
      b3.limit?.should_not be_nil
      b3.offset?.should_not be_nil

      b4.selects?.should_not be_nil
      b4.conditions?.should_not be_nil
      b4.orders?.should_not be_nil
      b4.limit?.should be_nil
      b4.offset?.should_not be_nil

      b5.selects?.should_not be_nil
      b5.conditions?.should_not be_nil
      b5.orders?.should_not be_nil
      b5.limit?.should_not be_nil
      b5.offset?.should be_nil
    end

    it "test unscope" do
      b = Query::Builder.new("foos")
        .select!(:id, :group_id)
        .where!(group_id: 1)
        .order!(:id)
        .limit!(10)
        .offset!(200)

      b.unscope!(:select)
      b.selects?.should be_nil
      b.conditions?.should_not be_nil
      b.orders?.should_not be_nil
      b.limit?.should_not be_nil
      b.offset?.should_not be_nil

      b.unscope!(:where)
      b.conditions?.should be_nil
      b.orders?.should_not be_nil
      b.limit?.should_not be_nil
      b.offset?.should_not be_nil

      b.unscope!(:order)
      b.orders?.should be_nil
      b.limit?.should_not be_nil
      b.offset?.should_not be_nil

      b.unscope!(:limit)
      b.limit?.should be_nil
      b.offset?.should_not be_nil

      b.unscope!(:offset)
      b.offset?.should be_nil
    end
  end

  describe "Query Optimization" do
    it "returns EXPLAIN ANALYZE output" do
      query = BasicModel.where(age: 10).limit(5)
      explain_output = query.explain

      # Should contain PostgreSQL EXPLAIN output
      explain_output.should contain("Seq Scan")
      explain_output.should_not be_empty
    end

    it "works with complex queries" do
      query = BasicModel.where(age: 10).order(:name).limit(10).offset(5)
      explain_output = query.explain

      explain_output.should contain("Limit")
      explain_output.should_not be_empty
    end

    it "works with joins" do
      author = Author.create!(name: "Test Author")
      Book.create!(name: "Test Book", author_id: author.id)

      query = Author.join(:left, Book, :author_id).where(id: author.id)
      explain_output = query.explain

      explain_output.should contain("Join")
      explain_output.should_not be_empty
    end

    it "works with full-text search" do
      Article.create!(title: "Crystal Programming", content: "Learn Crystal")

      query = Article.search("crystal", :title, :content)
      explain_output = query.explain

      # Should show the query plan
      explain_output.should_not be_empty
      explain_output.should be_a(String)
    end

    it "works with pagination" do
      5.times { |i| BasicModel.create!(name: "Test #{i}", age: i) }

      result = BasicModel.where(age: 1).paginate(page: 1, limit: 2)

      # Get the query and explain it
      query = BasicModel.where(age: 1).limit(2).offset(0)
      explain_output = query.explain

      explain_output.should_not be_empty
      explain_output.should contain("Limit")
    end
  end

  describe "in_groups_of" do
    before_each do
      BasicModel.clear
      10.times { |i| BasicModel.create!(name: "Model #{i}", age: i) }
    end

    it "batches records into groups of specified size" do
      groups = [] of Array(BasicModel | Nil)
      BasicModel.all.in_groups_of(3) do |group|
        groups << group
      end

      groups.size.should eq(4) # 10 records / 3 = 4 groups
      groups[0].size.should eq(3)
      groups[1].size.should eq(3)
      groups[2].size.should eq(3)
      groups[3].size.should eq(3) # Last group padded with nil
      groups[3][0].should be_a(BasicModel)
      groups[3][1].should be_nil
      groups[3][2].should be_nil
    end

    it "fills last group with specified value" do
      groups = [] of Array(BasicModel | String)
      BasicModel.all.in_groups_of(3, filled_up_with: "padding") do |group|
        groups << group
      end

      groups.size.should eq(4)
      groups[3][1].should eq("padding")
      groups[3][2].should eq("padding")
    end

    it "works with exact multiples" do
      BasicModel.clear
      9.times { |i| BasicModel.create!(name: "Model #{i}", age: i) }

      groups = [] of Array(BasicModel | Nil)
      BasicModel.all.in_groups_of(3) do |group|
        groups << group
      end

      groups.size.should eq(3)
      groups.each do |group|
        group.size.should eq(3)
        group.all?(BasicModel).should be_true
      end
    end

    it "works with single record" do
      BasicModel.clear
      BasicModel.create!(name: "Only", age: 1)

      groups = [] of Array(BasicModel | Nil)
      BasicModel.all.in_groups_of(3) do |group|
        groups << group
      end

      groups.size.should eq(1)
      groups[0].size.should eq(3)
      groups[0][0].should be_a(BasicModel)
      groups[0][1].should be_nil
      groups[0][2].should be_nil
    end

    it "raises error on zero size" do
      expect_raises(ArgumentError, "Size must be positive") do
        BasicModel.all.in_groups_of(0) { }
      end
    end

    it "raises error on negative size" do
      expect_raises(ArgumentError, "Size must be positive") do
        BasicModel.all.in_groups_of(-5) { }
      end
    end

    it "works with where clauses" do
      groups = [] of Array(BasicModel | Nil)
      BasicModel.where("age > ?", 5).in_groups_of(2) do |group|
        groups << group
      end

      groups.size.should eq(2) # 4 records (6,7,8,9) / 2 = 2 groups
      groups.all? { |g| g.all? { |item| item.nil? || item.as(BasicModel).age > 5 } }.should be_true
    end

    it "works with order clauses" do
      groups = [] of Array(BasicModel | Nil)
      BasicModel.order(age: :desc).in_groups_of(3) do |group|
        groups << group
      end

      # First group should have highest ages
      first_group = groups[0].compact
      first_group.first.age.should eq(9)
    end

    it "reuses array when reuse is true" do
      array_ids = [] of UInt64
      BasicModel.all.in_groups_of(3, reuse: true) do |group|
        array_ids << group.object_id
      end

      # All groups should use the same array object
      array_ids.uniq.size.should eq(1)
    end

    it "creates new arrays when reuse is false" do
      array_ids = [] of UInt64
      BasicModel.all.in_groups_of(3, reuse: false) do |group|
        array_ids << group.object_id
      end

      # Each group should be a different array object
      array_ids.uniq.size.should eq(array_ids.size)
    end
  end
end

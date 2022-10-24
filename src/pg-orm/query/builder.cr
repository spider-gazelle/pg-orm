require "uuid"

module PgORM
  alias Value = String | Nil | Bool | Int32 | Int64 | Float32 | Float64 | Time | UUID

  # :nodoc:
  struct Query::Builder
    struct Condition
      getter column_name : Symbol
      getter value : Value | Regex | Array(Value)
      property not : Bool

      def initialize(@column_name, @value, @not = false)
      end
    end

    struct RawCondition
      getter raw : String
      getter values : Array(Value)?
      property not : Bool

      def initialize(@raw, @values, @not = false)
      end
    end

    alias Selects = Array(Symbol | String)
    alias Conditions = Array(Condition | RawCondition)
    alias Orders = Array({Symbol, Symbol} | String)

    property table_name : String
    property primary_key : String
    property selects : Selects?
    property conditions : Conditions?
    property orders : Orders?
    property limit : Int32 = -1
    property offset : Int32 = -1

    def initialize(@table_name, @primary_key = "")
      @distinct = false
      @not = false
      @none = false
    end

    def selects? : Selects?
      return unless selects = @selects
      return if selects.empty?
      selects
    end

    def conditions? : Conditions?
      return unless conditions = @conditions
      return if conditions.empty?
      conditions
    end

    def orders? : Orders?
      return unless orders = @orders
      return if orders.empty?
      orders
    end

    def limit? : Int32?
      @limit unless @limit == -1
    end

    def offset? : Int32?
      @offset unless @offset == -1
    end

    def select(*columns : Symbol | String) : self
      builder = dup
      builder.selects = @selects.dup
      builder.select!(*columns)
      builder
    end

    def select!(*columns : Symbol | String) : self
      actual = @selects ||= Selects.new
      columns.each { |name| actual << name }
      self
    end

    def distinct(distinct = true) : self
      builder = dup
      builder.distinct!(distinct)
    end

    def distinct!(@distinct = true) : self
      self
    end

    def distinct? : Bool
      @distinct
    end

    def none : self
      builder = self.class.new(table_name, primary_key)
      builder.where!("1 = 0")
      builder.none = true
      builder
    end

    def none=(@none : Bool) : Bool
    end

    def none? : Bool
      @none
    end

    def where_not(*args, **opts)
      builder = dup
      builder.conditions = @conditions.dup
      builder._not { builder.where!(*args, **opts) }
    end

    def where_not!(*args, **opts)
      _not { where!(*args, **opts) }
    end

    protected def _not
      @not = true
      yield
    ensure
      @not = false
    end

    def where(conditions : Hash(Symbol, Value | Regex | Array(Value)) | NamedTuple) : self
      builder = dup
      builder.conditions = @conditions.dup
      builder.where!(conditions)
    end

    def where!(conditions : Hash(Symbol, Value | Regex | Array(Value)) | NamedTuple) : self
      actual = @conditions ||= Conditions.new
      conditions.each do |k, v|
        if v.is_a?(Enumerable)
          actual << Condition.new(k, v.map(&.as(Value)), @not)
        else
          actual << Condition.new(k, v, @not)
        end
      end
      @not = false
      self
    end

    def where(raw : String, *args) : self
      builder = dup
      builder.conditions = @conditions.dup
      builder.where!(raw, *args)
    end

    def where!(raw : String, *args) : self
      actual = @conditions ||= Conditions.new
      if args.empty?
        actual << RawCondition.new(raw, nil, @not)
      else
        values = Array(Value).new(args.size) { |i| args[i].as(Value) }
        actual << RawCondition.new(raw, values, @not)
      end
      @not = false
      self
    end

    def where(**conditions) : self
      where(conditions)
    end

    def where!(**conditions) : self
      where!(conditions)
    end

    def limit(value : Int32) : self
      builder = dup
      builder.limit!(value)
    end

    def limit!(@limit : Int32) : self
      self
    end

    def offset(value : Int32) : self
      builder = dup
      builder.offset!(value)
    end

    def offset!(@offset : Int32) : self
      self
    end

    def order(columns : Hash(Symbol, Symbol)) : self
      builder = dup
      builder.orders = @orders.dup
      builder.order!(columns)
    end

    def order!(columns : Hash(Symbol, Symbol)) : self
      actual = @orders ||= Orders.new
      columns.each { |name, direction| actual << {name, direction} }
      self
    end

    def order(*columns : Symbol | String) : self
      builder = dup
      builder.orders = @orders.dup
      builder.order!(*columns)
    end

    def order!(*columns : Symbol | String) : self
      actual = @orders ||= Orders.new
      columns.each do |value|
        case value
        when Symbol
          actual << {value, :asc}
        when String
          actual << value
        end
      end
      self
    end

    def order(**columns) : self
      builder = dup
      builder.orders = @orders.dup
      builder.order!(**columns)
    end

    def order!(**columns) : self
      actual = @orders ||= Orders.new
      columns.each { |name, direction| actual << {name, direction} }
      self
    end

    def reorder(columns : Hash(Symbol, Symbol)) : self
      builder = dup
      builder.orders = Orders.new
      builder.order!(columns)
    end

    def reorder!(columns : Hash(Symbol, Symbol)) : self
      @orders.try(&.clear)
      order!(columns)
    end

    def reorder(*columns : Symbol | String) : self
      builder = dup
      builder.orders = Orders.new
      builder.order!(*columns)
    end

    def reorder!(*columns : Symbol | String) : self
      @orders.try(&.clear)
      order!(*columns)
    end

    def reorder(**columns) : self
      builder = dup
      builder.orders = Orders.new
      builder.order!(**columns)
    end

    def reorder!(**columns) : self
      @orders.try(&.clear)
      order!(**columns)
    end

    def unscope(*args : Symbol) : self
      builder = dup
      builder.unscope!(*args)
    end

    def unscope!(*args : Symbol) : self
      args.each do |arg|
        case arg
        when :select then @selects = nil
        when :where  then @conditions = nil
        when :order  then @orders = nil
        when :limit  then @limit = -1
        when :offset then @offset = -1
        else              raise "unknown property to unscope: #{arg}"
        end
      end
      self
    end
  end
end

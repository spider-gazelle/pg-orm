require "json"
require "yaml"
require "./query/methods"
require "./query/cache"

module PgORM
  class Relation(T)
    include JSON::Serializable
    include YAML::Serializable
    include Enumerable(T)
    include Query::Methods(T)
    include Query::Cache(T)

    # :nodoc:
    def initialize(@parent : Base, @foreign_key : Symbol, @builder : Query::Builder? = nil)
    end

    def build(**attributes) : T
      record = T.new(**attributes)
      record[@foreign_key] = @parent.id?
      (@cache ||= [] of T) << record
      record
    end

    def create(**attributes) : T
      record = build(**attributes)
      record.save
      record
    end

    def delete(*records : T) : Nil
      ids = records.map(&.id)
      where({T.primary_key => ids.to_a}).delete_all
      @cache.try(&.reject! { |r| ids.includes?(r.id) })
    end

    protected def dup(builder : Query::Builder) : self
      Relation(T).new(@parent, @foreign_key, builder)
    end

    protected def builder
      @builder ||=
        if @parent.id?
          builder = Query::Builder.new(T.table_name, T.primary_key.to_s)
          builder.where!({@foreign_key => @parent.id})
          builder
        else
          raise Error::RecordNotSaved.new("can't initialize Relation(#{T.name}) for #{@parent.class.name} doesn't have an id.")
        end
    end

    delegate to_json, to_pretty_json, from_json, to: @parent
    delegate to_yaml, from_yaml, to: @parent
  end
end

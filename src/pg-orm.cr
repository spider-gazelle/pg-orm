module PgORM
  VERSION = {{ `shards version "#{__DIR__}"`.chomp.stringify.downcase }}
end

require "./pg-orm/**"

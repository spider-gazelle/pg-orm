require "uri"
require "habitat"

module PgORM
  module Settings
    @@url : String?

    Habitat.create do
      setting host : String = ENV["PG_HOST"]? || "localhost"
      setting port : Int32 = (ENV["PG_PORT"]? || 5432).to_i
      setting db : String = ENV["PG_DB"]? || ENV["PG_DATABASE"]? || "test"
      setting user : String = ENV["PG_USER"]? || "postgres"
      setting password : String = ENV["PG_PASSWORD"]? || ""
      setting query : String = ENV["PG_QUERY"]? || ""
    end

    def self.to_uri
      @@url ||= String.build do |sb|
        sb << "postgres://"
        sb << URI.encode_www_form(settings.user) unless settings.user.blank?
        sb << ":#{URI.encode_www_form(settings.password)}" unless settings.password.blank?
        sb << "@" unless settings.user.blank?
        sb << settings.host << ":" << settings.port
        sb << "/" unless settings.db.starts_with?("/")
        sb << settings.db
        sb << "?#{settings.query}" unless settings.query.blank?
      end
      @@url.not_nil!
    end

    def self.parse(uri : String | URI)
      uri = uri.is_a?(String) ? URI.parse(uri) : uri.as(URI)
      @@url = uri.to_s
      configure do |settings|
        settings.host = uri.host || "localhost"
        settings.port = uri.port || 5432
        settings.db = uri.path
        settings.user = uri.user || "postgres"
        settings.password = uri.password || ""
        settings.query = uri.query || ""
      end
    end
  end
end

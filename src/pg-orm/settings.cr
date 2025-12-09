require "uri"
require "habitat"

module PgORM
  # Database connection settings and configuration.
  #
  # Settings can be configured via environment variables or programmatically.
  # The module uses the Habitat shard for configuration management.
  #
  # ## Environment Variables
  #
  # - `PG_HOST`: Database host (default: "localhost")
  # - `PG_PORT`: Database port (default: 5432)
  # - `PG_DB` or `PG_DATABASE`: Database name (default: "test")
  # - `PG_USER`: Database user (default: "postgres")
  # - `PG_PASSWORD`: Database password (default: "")
  # - `PG_QUERY`: Additional query parameters (default: "")
  # - `PG_LOCK_TIMEOUT`: Advisory lock timeout in seconds (default: 5)
  #
  # ## Programmatic Configuration
  #
  # ```
  # # Configure individual settings
  # PgORM::Database.configure do |settings|
  #   settings.host = "db.example.com"
  #   settings.port = 5432
  #   settings.db = "production"
  #   settings.user = "app_user"
  #   settings.password = "secret"
  # end
  #
  # # Or parse a connection URL
  # PgORM::Database.parse("postgres://user:pass@localhost:5432/mydb")
  # ```
  module Settings
    @@url : String?

    Habitat.create do
      setting host : String = ENV["PG_HOST"]? || "localhost"
      setting port : Int32 = (ENV["PG_PORT"]? || 5432).to_i
      setting db : String = ENV["PG_DB"]? || ENV["PG_DATABASE"]? || "test"
      setting user : String = ENV["PG_USER"]? || "postgres"
      setting password : String = ENV["PG_PASSWORD"]? || ""
      setting query : String = ENV["PG_QUERY"]? || ""
      setting lock_timeout : Time::Span = (ENV["PG_LOCK_TIMEOUT"]? || 5).to_i.seconds
    end

    # Builds a PostgreSQL connection URI from the current settings.
    #
    # The URI is cached after first generation for performance.
    #
    # ## Example
    #
    # ```
    # PgORM::Settings.to_uri
    # # => "postgres://user:pass@localhost:5432/mydb"
    # ```
    def self.to_uri : String
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
    end

    # Parses a PostgreSQL connection URI and updates settings.
    #
    # This is useful for configuring the database from a single connection
    # string, such as from environment variables in production.
    #
    # ## Example
    #
    # ```
    # # From string
    # PgORM::Settings.parse("postgres://user:pass@localhost:5432/mydb")
    #
    # # From environment variable
    # PgORM::Settings.parse(ENV["DATABASE_URL"])
    #
    # # From URI object
    # uri = URI.parse("postgres://user:pass@localhost:5432/mydb")
    # PgORM::Settings.parse(uri)
    # ```
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

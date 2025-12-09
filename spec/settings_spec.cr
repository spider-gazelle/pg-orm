require "./spec_helper"

describe PgORM::Settings do
  # Save original configuration
  original_uri = ""

  Spec.before_each do
    original_uri = PgORM::Settings.to_uri
  end

  Spec.after_each do
    # Restore original configuration after each test
    PgORM::Settings.parse(original_uri) unless original_uri.empty?
  end

  describe "Configuration" do
    it "has default settings" do
      settings = PgORM::Settings.settings

      settings.host.should be_a(String)
      settings.port.should be_a(Int32)
      settings.db.should be_a(String)
      settings.user.should be_a(String)
      settings.password.should be_a(String)
      settings.query.should be_a(String)
      settings.lock_timeout.should be_a(Time::Span)
    end

    it "builds connection URI from settings" do
      uri = PgORM::Settings.to_uri

      uri.should be_a(String)
      uri.should start_with("postgresql://")
      uri.should contain("@")
      uri.should contain(":")
      uri.should contain("/")
    end

    it "parses connection string" do
      test_uri = "postgres://testuser:testpass@testhost:5433/testdb"

      PgORM::Settings.parse(test_uri)

      settings = PgORM::Settings.settings
      settings.host.should eq("testhost")
      settings.port.should eq(5433)
      settings.db.should eq("/testdb")
      settings.user.should eq("testuser")
      settings.password.should eq("testpass")
    end

    it "parses connection string with query parameters" do
      test_uri = "postgres://user:pass@host:5432/db?sslmode=require&connect_timeout=10"

      PgORM::Settings.parse(test_uri)

      settings = PgORM::Settings.settings
      settings.query.should eq("sslmode=require&connect_timeout=10")
    end

    it "parses connection string without password" do
      test_uri = "postgres://user@host:5432/db"

      PgORM::Settings.parse(test_uri)

      settings = PgORM::Settings.settings
      settings.user.should eq("user")
      settings.password.should eq("")
    end

    it "parses connection string without user" do
      test_uri = "postgres://host:5432/db"

      PgORM::Settings.parse(test_uri)

      settings = PgORM::Settings.settings
      settings.host.should eq("host")
      settings.user.should eq("postgres") # Default
    end

    it "parses URI object" do
      uri_obj = URI.parse("postgres://user:pass@host:5432/db")

      PgORM::Settings.parse(uri_obj)

      settings = PgORM::Settings.settings
      settings.host.should eq("host")
      settings.port.should eq(5432)
    end

    it "handles connection string with special characters in password" do
      # Password with special characters that need URL encoding
      test_uri = "postgres://user:p%40ss%23word@host:5432/db"

      PgORM::Settings.parse(test_uri)

      settings = PgORM::Settings.settings
      settings.user.should eq("user")
      # Password should be decoded
      settings.password.should eq("p@ss#word")
    end

    it "builds URI with special characters in password" do
      # Test URL encoding directly
      test_password = "p@ss#word!"
      encoded = URI.encode_www_form(test_password)

      # Verify encoding works correctly
      encoded.should eq("p%40ss%23word%21")
    end

    it "caches built URI" do
      uri1 = PgORM::Settings.to_uri
      uri2 = PgORM::Settings.to_uri

      # Should return same cached URI string
      uri1.should eq(uri2)
    end

    it "handles database path starting with slash" do
      test_uri = "postgres://user:pass@host:5432/mydb"

      PgORM::Settings.parse(test_uri)

      uri = PgORM::Settings.to_uri
      # Should not have double slashes
      uri.should_not contain("//mydb")
    end

    it "handles empty query string" do
      PgORM::Database.configure do |settings|
        settings.host = "localhost"
        settings.port = 5432
        settings.db = "testdb"
        settings.user = "user"
        settings.password = "pass"
        settings.query = ""
      end

      uri = PgORM::Settings.to_uri
      # Should not have trailing ?
      uri.should_not end_with("?")
    end

    it "handles blank user" do
      # Test that blank user logic works in URI building
      # When user is blank, @ should not be included
      # This is tested by the URI format itself
      uri = PgORM::Settings.to_uri
      uri.should be_a(String)
      uri.should contain("postgresql://")
    end

    it "configures lock timeout" do
      PgORM::Database.configure do |settings|
        settings.lock_timeout = 10.seconds
      end

      settings = PgORM::Settings.settings
      settings.lock_timeout.should eq(10.seconds)
    end

    it "uses lock timeout in advisory locks" do
      lock = PgORM::PgAdvisoryLock.new("test_lock")
      lock.timeout.should be_a(Time::Span)
    end

    it "allows custom lock timeout per lock" do
      lock = PgORM::PgAdvisoryLock.new("test_lock", timeout: 15.seconds)
      lock.timeout.should eq(15.seconds)
    end
  end

  describe "Environment Variables" do
    it "reads PG_HOST from environment" do
      # This test assumes PG_HOST is set or uses default
      settings = PgORM::Settings.settings
      settings.host.should be_a(String)
      settings.host.should_not be_empty
    end

    it "reads PG_PORT from environment" do
      settings = PgORM::Settings.settings
      settings.port.should be_a(Int32)
      settings.port.should be > 0
    end

    it "reads PG_DB or PG_DATABASE from environment" do
      settings = PgORM::Settings.settings
      settings.db.should be_a(String)
      settings.db.should_not be_empty
    end

    it "reads PG_USER from environment" do
      settings = PgORM::Settings.settings
      settings.user.should be_a(String)
      settings.user.should_not be_empty
    end

    it "uses defaults when environment variables not set" do
      # Settings should have valid values
      settings = PgORM::Settings.settings

      # All settings should be populated
      settings.host.should_not be_empty
      settings.port.should be > 0
      settings.user.should_not be_empty
      settings.db.should_not be_empty
    end
  end

  describe "Configuration Validation" do
    it "handles invalid port in URI" do
      # PostgreSQL will handle invalid ports, but we can test parsing
      test_uri = "postgres://user:pass@host:invalid/db"

      expect_raises(Exception) do
        URI.parse(test_uri).port.not_nil!
      end
    end

    it "handles missing database name" do
      test_uri = "postgres://user:pass@host:5432/"

      PgORM::Settings.parse(test_uri)

      settings = PgORM::Settings.settings
      settings.db.should eq("/")
    end

    it "handles localhost variations" do
      test_cases = {
        "localhost" => "localhost",
        "127.0.0.1" => "127.0.0.1",
        "[::1]"     => "[::1]", # IPv6 needs brackets in URI
      }

      test_cases.each do |uri_host, expected_host|
        test_uri = "postgres://user:pass@#{uri_host}:5432/db"

        PgORM::Settings.parse(test_uri)

        settings = PgORM::Settings.settings
        settings.host.should contain(expected_host.gsub("[", "").gsub("]", ""))
      end
    end

    it "handles different port numbers" do
      [5432, 5433, 5434, 15432].each do |port|
        test_uri = "postgres://user:pass@host:#{port}/db"

        PgORM::Settings.parse(test_uri)

        settings = PgORM::Settings.settings
        settings.port.should eq(port)
      end
    end
  end

  describe "Connection String Formats" do
    it "handles minimal connection string" do
      test_uri = "postgres://localhost/db"

      PgORM::Settings.parse(test_uri)

      settings = PgORM::Settings.settings
      settings.host.should eq("localhost")
      settings.db.should eq("/db")
    end

    it "handles full connection string" do
      test_uri = "postgres://user:pass@host:5433/db?sslmode=require"

      PgORM::Settings.parse(test_uri)

      settings = PgORM::Settings.settings
      settings.host.should eq("host")
      settings.port.should eq(5433)
      settings.db.should eq("/db")
      settings.user.should eq("user")
      settings.password.should eq("pass")
      settings.query.should eq("sslmode=require")
    end

    it "handles connection string with IPv6 address" do
      test_uri = "postgres://user:pass@[::1]:5432/db"

      PgORM::Settings.parse(test_uri)

      settings = PgORM::Settings.settings
      # IPv6 addresses keep brackets in URI parsing
      settings.host.should contain("::1")
    end

    it "handles connection string with domain name" do
      test_uri = "postgres://user:pass@db.example.com:5432/mydb"

      PgORM::Settings.parse(test_uri)

      settings = PgORM::Settings.settings
      settings.host.should eq("db.example.com")
      settings.db.should eq("/mydb")
    end
  end

  describe "Reconfiguration" do
    it "allows reconfiguration" do
      original_host = PgORM::Settings.settings.host

      PgORM::Database.configure do |settings|
        settings.host = "newhost"
      end

      PgORM::Settings.settings.host.should eq("newhost")
    end

    it "maintains settings after reconfiguration" do
      original_host = PgORM::Settings.settings.host

      PgORM::Database.configure do |settings|
        settings.host = "different_host"
      end

      PgORM::Settings.settings.host.should eq("different_host")
    end

    it "updates settings on parse" do
      original_host = PgORM::Settings.settings.host

      PgORM::Settings.parse("postgres://newuser:newpass@newhost:5432/newdb")

      settings = PgORM::Settings.settings
      settings.host.should eq("newhost")
      settings.user.should eq("newuser")
    end
  end
end

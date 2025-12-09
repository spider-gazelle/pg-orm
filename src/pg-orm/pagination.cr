module PgORM
  # Result wrapper for offset-based pagination containing records and metadata.
  #
  # Records are loaded lazily - the database query doesn't execute until you
  # access the records. This allows you to pass pagination results around
  # without loading data unnecessarily.
  #
  # ## Metadata Available
  #
  # - `total`: Total number of records matching the query
  # - `limit`: Number of records per page
  # - `offset`: Number of records skipped
  # - `page`: Current page number (1-indexed)
  # - `total_pages`: Total number of pages
  # - `has_next?`: Whether there's a next page
  # - `has_prev?`: Whether there's a previous page
  # - `next_page`: Next page number (or nil)
  # - `prev_page`: Previous page number (or nil)
  # - `from`: Starting record number (1-indexed)
  # - `to`: Ending record number (1-indexed)
  #
  # ## Example
  #
  # ```
  # result = User.where(active: true).paginate(page: 2, limit: 20)
  #
  # result.total       # => 150
  # result.page        # => 2
  # result.total_pages # => 8
  # result.has_next?   # => true
  # result.from        # => 21
  # result.to          # => 40
  #
  # # Records loaded only when accessed
  # result.records.each do |user|
  #   puts user.name
  # end
  #
  # # Or iterate directly
  # result.each do |user|
  #   puts user.name
  # end
  # ```
  struct PaginatedResult(T)
    getter total : Int64
    getter limit : Int32
    getter offset : Int32
    getter page : Int32

    @records : Array(T)?
    @query : Collection(T) | Relation(T)

    def initialize(@query : Collection(T) | Relation(T), @total : Int64, @limit : Int32, @offset : Int32)
      @page = (@offset // @limit) + 1
    end

    # Lazily load records only when accessed
    # Records are cached after first access to avoid multiple DB queries
    def records : Array(T)
      @records ||= @query.to_a
    end

    # Total number of pages
    def total_pages : Int32
      return 1 if @total == 0
      ((@total.to_f / @limit).ceil).to_i
    end

    # Whether there is a next page
    def has_next? : Bool
      @page < total_pages
    end

    # Whether there is a previous page
    def has_prev? : Bool
      @page > 1
    end

    # Next page number (nil if no next page)
    def next_page : Int32?
      has_next? ? @page + 1 : nil
    end

    # Previous page number (nil if no previous page)
    def prev_page : Int32?
      has_prev? ? @page - 1 : nil
    end

    # Starting record number (1-indexed)
    def from : Int32
      return 0 if @total == 0
      @offset + 1
    end

    # Ending record number (1-indexed)
    def to : Int32
      return 0 if @total == 0
      [@offset + @limit, @total.to_i].min
    end

    # Iterate over records without loading all into memory at once
    # This allows streaming/processing records one at a time
    def each(&block : T ->)
      @query.each(&block)
    end

    # Convert to JSON with pagination metadata
    # Note: This will load all records into memory for serialization
    def to_json(json : JSON::Builder)
      json.object do
        json.field "data" do
          json.array do
            records.each(&.to_json(json))
          end
        end
        json.field "pagination" do
          json.object do
            json.field "total", @total
            json.field "limit", @limit
            json.field "offset", @offset
            json.field "page", @page
            json.field "total_pages", total_pages
            json.field "has_next", has_next?
            json.field "has_prev", has_prev?
            json.field "next_page", next_page
            json.field "prev_page", prev_page
            json.field "from", from
            json.field "to", to
          end
        end
      end
    end
  end

  # Result wrapper for cursor-based pagination.
  #
  # Cursor pagination is more efficient than offset pagination for large datasets
  # because it doesn't require counting all records or skipping rows. Instead,
  # it uses the primary key (or another column) as a cursor to fetch the next
  # or previous page.
  #
  # ## Advantages over Offset Pagination
  #
  # - **Performance**: No OFFSET clause, which gets slower with large offsets
  # - **Consistency**: New records don't shift pages during pagination
  # - **Scalability**: Works well with millions of records
  #
  # ## Limitations
  #
  # - Can't jump to arbitrary pages (only next/previous)
  # - No total count or page numbers
  # - Requires a sortable cursor column (usually primary key)
  #
  # ## Example
  #
  # ```
  # # First page
  # result = Article.order(:id).paginate_cursor(limit: 20)
  # result.records.each { |article| puts article.title }
  #
  # # Next page (using cursor from previous result)
  # if result.has_next?
  #   next_result = Article.order(:id).paginate_cursor(
  #     after: result.next_cursor,
  #     limit: 20
  #   )
  # end
  #
  # # Previous page
  # if result.has_prev?
  #   prev_result = Article.order(:id).paginate_cursor(
  #     before: result.prev_cursor,
  #     limit: 20
  #   )
  # end
  # ```
  struct CursorPaginatedResult(T)
    getter limit : Int32
    getter next_cursor : String?
    getter prev_cursor : String?

    @records : Array(T)?
    @records_array : Array(T)

    def initialize(@records_array : Array(T), @limit : Int32, @next_cursor : String? = nil, @prev_cursor : String? = nil)
    end

    # Access records (already loaded for cursor determination)
    def records : Array(T)
      @records ||= @records_array
    end

    # Whether there is a next page
    def has_next? : Bool
      !@next_cursor.nil?
    end

    # Whether there is a previous page
    def has_prev? : Bool
      !@prev_cursor.nil?
    end

    # Iterate over records
    def each(&block : T ->)
      records.each(&block)
    end

    # Convert to JSON with cursor pagination metadata
    def to_json(json : JSON::Builder)
      json.object do
        json.field "data" do
          json.array do
            records.each(&.to_json(json))
          end
        end
        json.field "pagination" do
          json.object do
            json.field "limit", @limit
            json.field "has_next", has_next?
            json.field "has_prev", has_prev?
            if nc = @next_cursor
              json.field "next_cursor", nc
            else
              json.field "next_cursor", nil
            end
            if pc = @prev_cursor
              json.field "prev_cursor", pc
            else
              json.field "prev_cursor", nil
            end
          end
        end
      end
    end
  end

  # Pagination module providing offset-based and cursor-based pagination.
  #
  # ## Offset-Based Pagination
  #
  # Traditional page-number based pagination. Easy to use but can be slow
  # for large datasets or high page numbers.
  #
  # ```
  # # Page 2, 20 records per page
  # result = User.paginate(page: 2, limit: 20)
  #
  # # Or use offset directly
  # result = User.paginate_by_offset(offset: 40, limit: 20)
  # ```
  #
  # ## Cursor-Based Pagination
  #
  # More efficient for large datasets. Uses a cursor (usually the primary key)
  # to fetch the next/previous page.
  #
  # ```
  # # First page
  # result = User.order(:id).paginate_cursor(limit: 20)
  #
  # # Next page
  # result = User.order(:id).paginate_cursor(after: cursor, limit: 20)
  # ```
  module Pagination(T)
    # Paginates results using page number (1-indexed).
    #
    # This is the most common pagination method. It calculates the offset
    # automatically based on the page number and limit.
    #
    # ## Parameters
    #
    # - `page`: Page number (1-indexed, defaults to 1)
    # - `limit`: Records per page (defaults to 25)
    #
    # ## Example
    #
    # ```
    # # Get page 2 with 20 records per page
    # result = Article.where(published: true).paginate(page: 2, limit: 20)
    #
    # # With ordering
    # result = Article.order(:created_at).paginate(page: 1, limit: 10)
    #
    # # Works with joins (uses DISTINCT count automatically)
    # result = Article.join(:left, Comment, :article_id).paginate(page: 1, limit: 10)
    #
    # # Access metadata
    # puts "Page #{result.page} of #{result.total_pages}"
    # puts "Showing #{result.from}-#{result.to} of #{result.total}"
    #
    # # Iterate records
    # result.records.each do |article|
    #   puts article.title
    # end
    # ```
    #
    # Returns a `PaginatedResult` with records and metadata.
    def paginate(page : Int32 = 1, limit : Int32 = 25) : PaginatedResult(T)
      page = 1 if page < 1
      limit = 1 if limit < 1

      offset = (page - 1) * limit

      # Get total count (handles joins properly)
      total = paginate_count

      # Create query for paginated records (lazy - not executed until accessed)
      query = self.limit(limit).offset(offset)

      PaginatedResult.new(query, total, limit, offset)
    end

    # Paginates results using offset and limit directly.
    #
    # Use this when you want to control the offset manually instead of
    # using page numbers. Useful for custom pagination logic.
    #
    # ## Parameters
    #
    # - `offset`: Number of records to skip (defaults to 0)
    # - `limit`: Number of records to return (defaults to 25)
    #
    # ## Example
    #
    # ```
    # # Skip first 40 records, get next 20
    # result = Article.paginate_by_offset(offset: 40, limit: 20)
    #
    # # Custom pagination logic
    # offset = calculate_custom_offset()
    # result = Article.paginate_by_offset(offset: offset, limit: 20)
    # ```
    #
    # Returns a `PaginatedResult` with records and metadata.
    def paginate_by_offset(offset : Int32 = 0, limit : Int32 = 25) : PaginatedResult(T)
      offset = 0 if offset < 0
      limit = 1 if limit < 1

      # Get total count (handles joins properly)
      total = paginate_count

      # Create query for paginated records (lazy - not executed until accessed)
      query = self.limit(limit).offset(offset)

      PaginatedResult.new(query, total, limit, offset)
    end

    # Smart count that handles joins properly
    # For queries with joins, uses COUNT(DISTINCT primary_key) to avoid duplicates
    private def paginate_count : Int64
      builder = self.builder

      # Create a clean builder for counting
      count_builder = builder.dup
      count_builder.orders = nil
      count_builder.groups = nil
      count_builder.limit = -1
      count_builder.offset = -1
      count_builder.selects = nil
      count_builder.fts_rank_column = nil

      # Check if query has joins
      if builder.joins?
        # Use DISTINCT count on primary key for joined queries with table qualification
        table_name = builder.table_name
        pk = builder.primary_key
        qualified_pk = "#{table_name}.#{pk}"
        dup(count_builder).count(qualified_pk, distinct: true)
      else
        # Regular count for simple queries
        dup(count_builder).count
      end
    end

    # Cursor-based pagination for efficient large dataset traversal.
    #
    # This method uses a cursor (typically the primary key) to fetch pages
    # without using OFFSET, making it much more efficient for large datasets.
    #
    # ## Parameters
    #
    # - `after`: Cursor to fetch records after (for next page)
    # - `before`: Cursor to fetch records before (for previous page)
    # - `limit`: Number of records to return (defaults to 25)
    # - `cursor_column`: Column to use as cursor (defaults to :id)
    #
    # ## Example
    #
    # ```
    # # First page
    # result = Article.order(:id).paginate_cursor(limit: 20)
    #
    # result.records.each { |article| puts article.title }
    #
    # # Next page using cursor from previous result
    # if result.has_next?
    #   next_result = Article.order(:id).paginate_cursor(
    #     after: result.next_cursor,
    #     limit: 20
    #   )
    # end
    #
    # # Previous page using cursor
    # if result.has_prev?
    #   prev_result = Article.order(:id).paginate_cursor(
    #     before: result.prev_cursor,
    #     limit: 20
    #   )
    # end
    #
    # # Custom cursor column
    # result = Article.order(:created_at).paginate_cursor(
    #   limit: 20,
    #   cursor_column: :created_at
    # )
    # ```
    #
    # ## Important Notes
    #
    # - Always use `.order()` with the same column as `cursor_column`
    # - The cursor column should be indexed for performance
    # - Don't use both `after` and `before` at the same time
    #
    # Returns a `CursorPaginatedResult` with records and cursor metadata.
    def paginate_cursor(
      after : String? = nil,
      before : String? = nil,
      limit : Int32 = 25,
      cursor_column : Symbol = :id,
    ) : CursorPaginatedResult(T)
      limit = 1 if limit < 1

      query = self

      # Apply cursor conditions
      if after
        query = query.where("#{cursor_column} > ?", after)
      elsif before
        query = query.where("#{cursor_column} < ?", before)
      end

      # Fetch one extra record to determine if there's a next page
      records = query.limit(limit + 1).to_a

      has_more = records.size > limit
      records = records[0...limit] if has_more

      # Determine cursors
      next_cursor = nil
      prev_cursor = nil

      if !records.empty?
        if after || (!after && !before)
          # Forward pagination
          next_cursor = records.last.id.to_s if has_more
          prev_cursor = records.first.id.to_s if after
        else
          # Backward pagination
          prev_cursor = records.first.id.to_s if has_more
          next_cursor = records.last.id.to_s if before
        end
      end

      CursorPaginatedResult.new(records, limit, next_cursor, prev_cursor)
    end
  end
end

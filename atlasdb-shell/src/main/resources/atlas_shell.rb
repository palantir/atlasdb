# -*- mode: ruby; fill-column: 100; comment-column: 70; ruby-deep-indent-paren-style: nil; -*-
# vim: ts=2 sts=2 sw=2

require 'irb'
require 'irb/completion'
require 'java'
require 'okjson'
require 'pp'
require 'pathname'
require 'ostruct'

java_import com.google.common.collect.ImmutableSet
java_import com.google.common.collect.Maps
java_import com.google.common.collect.Multimap
java_import com.palantir.atlasdb.shell.AtlasShellRubyHelpers
java_import com.palantir.atlasdb.shell.AtlasShellRubyHelpers::ProtoToMapHelper
java_import com.palantir.atlasdb.keyvalue.api.Cell
java_import com.palantir.atlasdb.keyvalue.api.RangeRequests
java_import com.palantir.atlasdb.encoding.PtBytes
java_import com.google.common.io.BaseEncoding
java_import com.palantir.atlasdb.protos.generated.TableMetadataPersistence::ValueByteOrder

# For streams
java_import com.palantir.atlasdb.stream.GenericStreamStore

# For interactively prompting connection info
java_import com.palantir.atlasdb.shell.AtlasShellConnectionType

# For configuring chunking strategy when performing parallel range requests
java_import com.palantir.atlasdb.table.description.ValueType

module AtlasDBShell

  class Connection
    def inspect; to_s end
    def to_s
      "Connection<>"
    end

    attr_reader :write_transactions_enabled

    # Connect to a blank in-memory key-value store; supports full snapshot transactions
    def self.snapshot_in_memory
      Connection.new $atlas_shell_connection_factory.withSnapshotTransactionManagerInMemory, true
    end

    def self.snapshot_from_rocksdb path
      Connection.new $atlas_shell_connection_factory.withTransactionManagerRocksDb(path), true
    end

    def self.readonly_from_cassandra host, port, identifier
      Connection.new $atlas_shell_connection_factory.withReadOnlyTransactionManagerCassandra(host, port, identifier), false
    end

    # Names of Atlas tables
    def table_names
      @atlas_shell_connection.getTableNames.collect do |table_name|
        table_name
      end
    end

    # Retrieve a Table object by name
    def table table_name
      table_metadata = @atlas_shell_connection.getTableMetadata table_name
      Table.new self, table_name, table_metadata
    end

    # Run a snapshot transaction with retry
    def run &block
      @atlas_shell_connection.runTaskWithRetry do |transaction_adapter|
        transaction_adapter_adapter = Transaction.new transaction_adapter, self
        block.call transaction_adapter_adapter
      end
    end

    # Try to run a snapshot transaction once; throw an exception on failure
    def try &block
      @atlas_shell_connection.runTaskThrowOnConflict do |transaction_adapter|
        transaction_adapter_adapter = Transaction.new transaction_adapter, self
        block.call transaction_adapter_adapter
      end
    end

    # Try to run a read-only transaction once
    def read &block
      @atlas_shell_connection.runTaskReadOnly do |transaction_adapter|
        transaction_adapter_adapter = Transaction.new transaction_adapter, self
        block.call transaction_adapter_adapter
      end
    end

    def get_all_timestamps table_name, cells
      @atlas_shell_connection.getAllTimestamps(table_name, ImmutableSet.copyOf(cells.to_java))
    end

    # Return a transaction that performs no checks whatsoever
    def unsafe
      Transaction.new @atlas_shell_connection.unsafeReadOnlyTransaction, self
    end

    def objects
      @objects ||= Objects.new self
    end

    def streams
      @streams ||= Streams.new self
    end

    def method_missing method, *args, &block
      puts "Unknown method #{method} on #{self.class}"
      puts "Type 'help' for help with commands."
    end

    private

    def initialize atlas_shell_connection, write_transactions_enabled
      @atlas_shell_connection = atlas_shell_connection
      @atlas_shell_connection.setAtlasShellRuby $atlas_shell_ruby
      @write_transactions_enabled = write_transactions_enabled
    end
  end

  class Transaction
    def inspect; to_s end
    def to_s
      "Transaction<#{is_uncommitted.inspect}>"
    end

    def initialize transaction_adapter, connection
      @connection = connection
      @transaction_adapter = transaction_adapter
    end

    # Retrieve a single row
    def get_row table, row
      range = get_range_prefix(table, row, nil, 1) # no column selection specified, batch_size = 1
      range.first
    end

    # Retrieve a range of rows by specifying a lower bound (inclusive) and an upper bound (exclusive),
    # along with an optional column selection and an optional batch size specification
    def get_range table, start_row_inclusive, end_row_exclusive, columns=nil, batch_size=1000
      start_bytes = table._row_obj_to_bytes(start_row_inclusive)
      end_bytes = table._row_obj_to_bytes(end_row_exclusive)
      RowRange.new(@connection, @transaction_adapter, table, start_bytes, end_bytes, columns, batch_size)
    end

    # Retrieve a range of rows by specifying a prefix common to all of them, along with an
    # optional column selection and an optional batch size specification
    def get_range_prefix table, row_prefix, columns=nil, batch_size=1000
      start_bytes = table._row_obj_to_bytes(row_prefix)
      end_bytes = AtlasShellRubyHelpers.getEndRowForPrefixRange(start_bytes)
      RowRange.new(@connection, @transaction_adapter, table, start_bytes, end_bytes, columns, batch_size)
    end

    # Delete the columns in $column_obj_lst from the row $row_obj in $table
    def delete table, row_obj, column_obj_lst
      if @connection.write_transactions_enabled
        row_bytes = table._row_obj_to_bytes(row_obj)
        column_bytes_lst = column_obj_lst.map do |column_obj|
          table._column_obj_to_column_bytes(column_obj)
        end
        @transaction_adapter.delete(table.name, row_bytes, column_bytes_lst)
      else
        $stderr.puts "Delete failed; write transactions are not enabled for the current connection."
      end
    end

    # Set the value of each columns $column_obj in $column_obj_to_value_obj from the row $row_obj in $table
    # to $column_obj_to_value_obj[$column_obj]
    def put table, row_obj, column_obj_to_value_obj
      if @connection.write_transactions_enabled
        row_bytes = table._row_obj_to_bytes(row_obj)
        column_bytes_to_value_bytes = Maps.newHashMap
        column_obj_to_value_obj.map do |column_obj, value_obj|
          column_bytes, value_bytes = \
          table._column_obj_and_value_obj_to_column_bytes_and_value_bytes(column_obj, value_obj)
          column_bytes_to_value_bytes.put(column_bytes, value_bytes)
        end
        @transaction_adapter.put(table.name, row_bytes, column_bytes_to_value_bytes)
      else
        $stderr.puts "Put failed; write transactions are not enabled for the current connection."
      end
    end

    # Commit this transaction (this also happens automatically if nothing goes wrong)
    def commit
      @transaction_adapter.commit
    end

    # Abort this transaction (this also happens automatically if an exception is thrown)
    def abort
      @transaction_adapter.abort
    end

    def is_uncommitted
      @transaction_adapter.isUncommitted
    end

    # Retrieve the timestamp for this transaction; note that this is the start timestamp (not the commit timestamp)
    def get_timestamp
      @transaction_adapter.getTimestamp
    end

    def method_missing method, *args, &block
      puts "Unknown method #{method} on #{self.class}"
      puts "Type 'help' for help with commands."
    end
  end

  class Table
    include Enumerable
    def inspect; to_s end
    attr_reader :name, :metadata

    def to_s
      "Table<#{@name}>"
    end

    def initialize connection, table_name, table_metadata
      @connection = connection
      @name = table_name
      @metadata = table_metadata
    end

    # Shorthand for a transaction consisting only of a get_row on one table
    def get_row row
      @connection.unsafe.get_row(self, row)
    end

    # Shorthand for a transaction consisting only of a get_range on one table;
    # also supports optional column selection and batch size specification
    def get_range start_row_inclusive, end_row_exclusive, columns=nil, batch_size=1000
      @connection.unsafe.get_range(self, start_row_inclusive, end_row_exclusive, columns, batch_size)
    end

    # Shorthand for a transaction consisting only of a get_range_prefix on one table;
    # also supports optional column selection and batch size specification
    def get_range_prefix row_prefix, columns=nil, batch_size=1000
      @connection.unsafe.get_range_prefix(self, row_prefix, columns, batch_size)
    end

    # Shorthand for a transaction consisting only of a delete on one table
    def delete row_obj, column_obj_lst
      if @connection.write_transactions_enabled
        @connection.run {|tx| tx.delete self, row_obj, column_obj_lst}
      else
        $stderr.puts "Delete failed; write transactions are not enabled for the current connection."
      end
    end

    # Shorthand for a transaction consisting only of a put on one table
    def put row_obj, column_obj_to_value_obj
      if @connection.write_transactions_enabled
        @connection.run {|tx| tx.put self, row_obj, column_obj_to_value_obj}
      else
        $stderr.puts "Put failed; write transactions are not enabled for the current connection."
      end
    end

    # Note: The following methods ('first', 'take', 'each', 'view') are simply provided for
    # convenience; if you want to be able to control the column selection and the batch size,
    # then use a RowRange object directly

    # Shorthand for a transaction retrieving first row of table
    def first
      @connection.unsafe.get_range(self, nil, nil, nil, 1).first # no column selection specified, batch_size = 1
    end

    # Shorthand for a transaction retrieving rows of table
    # Note: you can use limit=-1 for all rows (do this at your own risk)
    def take limit=100
      @connection.unsafe.get_range(self, nil, nil).take(limit)
    end

    # Shorthand for a transaction performing an action on rows of a table
    # Note: unlike normal each, this can have a limit
    def each limit=-1, &block
      @connection.unsafe.get_range(self, nil, nil).each(limit, &block)
    end

    # Shorthand for a transaction viewing rows of a table
    # Note: you can use limit=-1 for all rows (do this at your own risk)
    def view limit=100
      @connection.unsafe.get_range(self, nil, nil).view(limit)
    end

    # Retrieve column schema
    def column_names
      if @metadata.getColumns.hasDynamicColumns
        puts "Table has dynamic columns."
        []
      else
        @metadata.getColumns.getNamedColumns.collect {|nc| nc.getLongName}
      end
    end

    # Retrieve row schema
    def row_components
      @metadata.getRowMetadata.getRowParts.collect {|c| c.getComponentName}
    end

    def reverse_ordered_row_components
      @metadata.getRowMetadata.getRowParts.select {|c| c.isReverseOrder }.collect {|c| c.getComponentName}
    end

    # Simple description of table
    def describe
      puts "Table name:\n  #{@name}"
      puts "Rows components:\n  #{row_components.inspect}"
      rev = reverse_ordered_row_components
      if rev.any?
        puts "Reverse ordered row components:\n #{rev.inspect}"
      end
      if @metadata.getColumns.hasDynamicColumns
        puts "Table has dynamic columns."
      else
        puts "Columns:\n  #{column_names.inspect}"
      end
    end

    # TODO(jaapweel): implement this stuff on the Java side

    def decodeHex hex
      return BaseEncoding.base16().lowerCase().decode(hex);
    end

    def encodeHex bytes
      return BaseEncoding.base16().lowerCase().encode(bytes);
    end

    def hex_to_row_name hex
      java_bytes = decodeHex(hex)
      return @metadata.getRowMetadata.renderToJson(java_bytes)
    end

    def hex_to_column_name hex
      if @metadata.getColumns.getDynamicColumn
        java_bytes = decodeHex(hex)
        dynamic_column_description =  @metadata.getColumns.getDynamicColumn
        name_metadata_desc = dynamic_column_description.getColumnNameDesc
        return name_metadata_desc.renderToJson(java_bytes)
      end
      @metadata.getColumns.getNamedColumns.each do |named_column_description|
        short_name = named_column_description.getShortName
        long_name = named_column_description.getLongName
        short_bytes = short_name.bytes.to_a.to_java(:byte)
        short_hex = encodeHex(short_bytes)
        if short_hex == hex
          return long_name
        end
      end
      raise(ArgumentError, "cannot find column with name #{hex}")
    end

    def hex_to_column_value hex, column_name = nil
      java_bytes = decodeHex(hex)
      if @metadata.getColumns.getDynamicColumn
        dynamic_column_description =  @metadata.getColumns.getDynamicColumn
        column_value_description = dynamic_column_description.getValue
        return ProtoToMapHelper.asMapOrString(column_value_description, java_bytes)
      end

      @metadata.getColumns.getNamedColumns.each do |named_column_description|
        long_name = named_column_description.getLongName
        if (column_name == long_name) || (@metadata.getColumns.getNamedColumns.size == 1 && column_name.nil?)
          column_value_description = named_column_description.getValue
          return ProtoToMapHelper.asMapOrString(column_value_description, java_bytes)
        end
        raise(ArgumentError, "cannot find column with name #{column_name}")
      end
    end

    def row_name_to_hex row_obj
      java_bytes = _row_obj_to_bytes(row_obj)
      return nil if java_bytes.nil?
      encodeHex(java_bytes)
    end

    def column_name_to_hex column_obj
      java_bytes = _column_obj_to_column_bytes(column_obj)
      return nil if java_bytes.nil?
      encodeHex(java_bytes)
    end

    def column_value_to_hex value_obj, column_name = nil
      column_bytes, value_bytes = _column_obj_and_value_obj_to_column_bytes_and_value_bytes(column_name, value_obj)
      return nil if value_bytes.nil?
      encodeHex(value_bytes)
    end

    def _row_obj_to_bytes row_obj
      return nil if row_obj.nil?
      row_json = _obj_to_json(row_obj)
      row_bytes = @metadata.getRowMetadata.parseFromJson(row_json, true)
      row_bytes
    end

    def _value_obj_to_bytes value_obj, column_value_description
      return nil if value_obj.nil?
      value_json = _obj_to_json(value_obj)
      column_value_description.persistJsonToBytes(value_json)
    end

    def _column_obj_to_column_bytes column_obj
      column_bytes, _ = _column_obj_and_value_obj_to_column_bytes_and_value_bytes(column_obj, nil)
      column_bytes
    end

    def _column_obj_and_value_obj_to_column_bytes_and_value_bytes column_obj, value_obj

      if @metadata.getColumns.getDynamicColumn
        column_json = _obj_to_json(column_obj);
        dynamic_column_description =  @metadata.getColumns.getDynamicColumn
        dynamic_column_name_description =  dynamic_column_description.getColumnNameDesc
        column_value_description = dynamic_column_description.getValue
        value_bytes = _value_obj_to_bytes(value_obj, column_value_description)

        column_bytes = nil
        if column_json
          column_bytes = dynamic_column_name_description.parseFromJson(column_json, true)
        end

        return column_bytes, value_bytes
      end

      @metadata.getColumns.getNamedColumns.each do |named_column_description|
        short_name = named_column_description.getShortName
        long_name = named_column_description.getLongName
        column_value_description = named_column_description.getValue
        if (column_obj.to_s == long_name) || (@metadata.getColumns.getNamedColumns.size == 1 && column_obj.nil?)
          column_bytes = short_name.bytes.to_a.to_java(:byte)
          value_bytes = _value_obj_to_bytes(value_obj, column_value_description)
          return column_bytes, value_bytes
        end
      end
    end

    def _obj_to_json obj
      if obj.is_a?(Hash)
        obj = Hash[obj.collect {|k, v| [k.to_s, v]}]
        obj = OkJson.encode obj
      end
      obj
    end

    def method_missing method, *args, &block
      puts "Unknown method #{method} on #{self.class}"
      puts "Type 'help' for help with commands."
    end
  end # end Table class

  def json_byte_string_to_hex json_byte_string
    return AtlasShellRubyHelpers.convertJsonByteStringIntoHex(json_byte_string)
  end

  class RowRange
    include Enumerable
    def inspect; to_s end
    def to_s
      "RowRange<table=#{@table.name} start_row_inclusive=#{@start_row_inclusive.inspect} end_row_exclusive=#{@end_row_exclusive.inspect} columns=#{@columns.inspect} batch_size=#{@batch_size}>"
    end

    def verify_columns
      if @columns == nil || @columns.length == 0
        return true
      end

      if @table.metadata.getColumns.hasDynamicColumns
        $stderr.puts "Invalid column selection: #{@columns.inspect}"
        $stderr.puts "Cannot specify a column selection because table #{@table.name}"
        $stderr.puts "has dynamic columns"
        return false
      end

      valid_names = @table.column_names
      for column in @columns
        if !valid_names.include?(column)
          $stderr.puts "Invalid column selection: #{@columns.inspect}"
          $stderr.puts "Table #{@table.name} only has the following columns:"
          $stderr.puts "  #{@table.column_names.inspect}"
          return false
        end
      end
      true
    end

    def initialize connection, transaction_adapter, table, start_row_inclusive=nil, end_row_exclusive=nil, columns=nil, batch_size=100
      @connection = connection
      @transaction_adapter = transaction_adapter
      @table = table
      @start_row_inclusive = start_row_inclusive
      @end_row_exclusive = end_row_exclusive
      @columns = columns
      @batch_size = batch_size

      if !verify_columns
        raise(ArgumentError, "Invalid column selection #{@columns.inspect} for table #{@table.name} with columns #{@table.column_names.inspect}")
      end

      if @batch_size <= 0
        raise(ArgumentError, "Invalid batch size #{@batch_size}; batch size must be positive")
      end

    end

    def first
      take(1)[0]
    end

    # Note: you can use limit=-1 for all rows (do this at your own risk)
    def take limit=100
      rows = []
      each(limit) {|row| rows.push row}
      rows
    end

    # Unlike "take", which leaves the RowRange object unchanged after
    # being called, the "take!" method modifies the RowRange object in
    # place by advancing @start_row_inclusive, so that calling "take!"
    # repeatedly will allow you to page through the entire range
    # Note: you can use limit=-1 for all rows (do this at your own risk)
    def take! limit=100
      rows = []
      if limit < 0
        each(limit) {|row| rows.push row}
      elsif limit == 0
        return rows
      else
        each(limit + 1) {|row| rows.push row}
      end

      if rows.size == 0
        # Range is already empty, do nothing
      elsif limit == -1 or rows.size <= limit
        # Reached the end of the range
        last_row_bytes = @table._row_obj_to_bytes(rows[-1].row_name)
        @start_row_inclusive = RangeRequests.getNextStartRow(false, last_row_bytes)
      else
        @start_row_inclusive = @table._row_obj_to_bytes(rows.pop.row_name)
      end
      rows
    end

    def short_column_names
      result = []
      if !(@columns.nil? || @columns.empty?)
        column_name_mapping = {}
        @table.metadata.getColumns.getNamedColumns.each {|nc| column_name_mapping[nc.getLongName] = nc.getShortName}
        result = @columns.collect {|long_column_name| column_name_mapping[long_column_name]}
      end
      result
    end

    # Note: you can limit how much each will iterate over but the default is everything (limit=-1)
    def each limit=-1, &block
      n = limit

      @transaction_adapter.each(
        @table.name,
        @start_row_inclusive,
        @end_row_exclusive,
        short_column_names.to_java(:String),
        @batch_size,
        Proc.new do |raw_row|
          n -= 1
          yield Row.new @connection, @table, raw_row
          limit < 0 || n > 0
        end)
    end

    # This method only works on prefix ranges.
    #
    # WARNING:
    #     You are responsible for managing concurrency yourself!  In particular,
    #     if you are accessing shared memory from within the block that you pass to
    #     this method, then you had better do some sort of locking.
    #
    #     For perf reasons, I recommend instantiating and using some sort of Java lock
    #     (e.g. a ReentrantReadWriteLock) as opposed to using Ruby locks.
    #
    # Note:
    #     Because we can't make any guarantees about the order in which threads
    #     execute requests, this call doesn't support a "limit" parameter.
    def each_prefix_parallel num_threads=8, value_type=ValueType::FIXED_LONG, &block
      @transaction_adapter.eachPrefixParallel(
        @table.name,
        @start_row_inclusive,
        @end_row_exclusive,
        short_column_names.to_java(:String),
        @batch_size,
        num_threads,
        value_type,
        Proc.new do |raw_row|
          yield Row.new @connection, @table, raw_row
          true # since there's no limit, we always continue
        end)
    end

    # Note: you can use limit=-1 for all rows (do this at your own risk)
    def view limit=100
      limited_rows = take(limit + 1)
      raw_rows = take(limit).collect {|r| r.raw_row}
      if $atlas_shell_gui_callback.isGraphicalViewEnabled
        $atlas_shell_gui_callback.graphicalView(@table.metadata, @table.name, @columns, raw_rows, limited_rows.count > limit)
      else
        puts "view method is not supported in headless mode"
        puts "\nInstead, obtain an array of row objects (e.g. using $table_name.take),"
        puts "and then for each row object r:"
        puts "    r.row_name to obtain the row name"
        puts "    r.columns to obtain the column values"
      end
    end

    def count
      result = java.util.concurrent.atomic.AtomicInteger.new
      if @transaction_adapter.isPrefixRange(@start_row_inclusive, @end_row_exclusive)
        each_prefix_parallel { result.getAndIncrement() }
      else
        each { result.getAndIncrement() }
      end
      result.get
    end

    def method_missing method, *args, &block
      puts "Unknown method #{method} on #{self.class}"
      puts "Type 'help' for help with commands."
    end
  end

  class Row
    attr_reader :table
    attr_reader :raw_row

    def inspect; to_s end
    def to_s
      "Row<#{@table.name} #{row_name.inspect}>"
    end

    def initialize connection, table, raw_row
      @connection = connection
      @table = table
      @raw_row = raw_row
    end

    # Retrieve the row name (decoded)
    def row_name
      @name ||= OkJson.decode @table.metadata.getRowMetadata.renderToJson(@raw_row.getRowName)
    end

    def row_name_hex
      encodeHex(@raw_row.getRowName)
    end

    def column_names_hex
      if @table.metadata.getColumns.hasDynamicColumns
        d = @table.metadata.getColumns.getDynamicColumn
        es = @raw_row.getColumns.collect do |k,v|
          key = OkJson.decode(d.getColumnNameDesc.renderToJson(k))
          value = encodeHex(k)
          [key, value]
        end
        Hash[es]
      else
        Hash[
          @table.metadata.getColumns.getNamedColumns.collect do |nc|
            [nc.getLongName,
              encodeHex(PtBytes.toBytes(nc.getShortName))
            ]
          end
        ]
      end
    end

    def timestamps
      row_bytes = @raw_row.getRowName
      columns_to_cells = {}
      if @table.metadata.getColumns.hasDynamicColumns
        d = @table.metadata.getColumns.getDynamicColumn
        es = @raw_row.getColumns.collect do |k,v|
          key = OkJson.decode(d.getColumnNameDesc.renderToJson(k))
          value = Cell.create(row_bytes, k)
          [key, value]
        end
        columns_to_cells = Hash[es]
      else
        columns_to_cells = Hash[
          @table.metadata.getColumns.getNamedColumns.collect do |nc|
            [nc.getLongName,
              Cell.create(row_bytes, PtBytes.toBytes(nc.getShortName))
            ]
          end
        ]
      end
      cells_to_timestamps = @connection.get_all_timestamps(@table.name, columns_to_cells.values)
      Hash[
        columns_to_cells.keys.collect do |col|
          c = columns_to_cells[col]
          ts = cells_to_timestamps.get(c).collect
          [col, ts]
        end
      ]
    end

    # View this row by itself
    def view
      if $atlas_shell_gui_callback.isGraphicalViewEnabled
        $atlas_shell_gui_callback.graphicalView(@table.metadata, @table.name, nil, [@raw_row], false)
      else
        puts "view method is not supported in headless mode"
        puts "\nInstead, obtain a row object r, and then:"
        puts "    r.row_name to obtain the row name"
        puts "    r.columns to obtain the column values"
      end
    end

    def column name

      # Check that the column actually exists
      column_description = nil
      @table.metadata.getColumns.getNamedColumns.each do |description|
        if description.getLongName == name
          if column_description != nil
            $stderr.puts "Invalid table metadata: Column #{name} appears more than once in metadata for table #{@table.name}"
            return
          end
          column_description = description
        end
      end

      if column_description == nil
        puts "Column #{name} does not exist in table #{@table.name}"
        puts "Allowed columns: #{@table.column_names.join(", ")}"
        return
      end

      name_bytes = PtBytes.toBytes(column_description.getShortName)
      value_bytes = @raw_row.getColumns.get(name_bytes)
      value_description = column_description.getValue

      return obj_to_ruby(ProtoToMapHelper.asMapOrString(value_description, value_bytes))
    end

    # Access individual values in this row by column name (returns a column->value hash)
    def columns
      if @table.metadata.getColumns.hasDynamicColumns
        @columns ||= dynamic_columns
      else
        @columns ||= _named_columns
      end
    end

    def _named_columns
      Hash[
        @table.metadata.getColumns.getNamedColumns.collect do |nc|
          [nc.getLongName,
            obj_to_ruby(ProtoToMapHelper.asMapOrString(nc, @raw_row))
          ]
        end
      ]
    end

    private

    def dynamic_columns
      d = @table.metadata.getColumns.getDynamicColumn
      es = @raw_row.getColumns.collect do |k,v|
        key = OkJson.decode(d.getColumnNameDesc.renderToJson(k))
        value = obj_to_ruby(ProtoToMapHelper.asMapOrString(d.getValue, v))
        [key, value]
      end
      Hash[es]
    end

    def method_missing method, *args, &block
      column method.to_s
    end
  end

  class Objects
    def initialize connection
      @connection = connection
      @obj_fragment = connection.table 'obj_fragment'
      @obj_hist_version = connection.table 'obj_hist_version'
    end

    def load rid, oid, dei
      all_fragments = get_fragments(rid, oid, dei)
      if all_fragments.nil?
        $stderr.puts "Could not find any fragments with the specified information: {'realm_id'=>#{rid}, 'object_id'=>#{oid}, 'data_event_id'=>#{dei}}"
        return nil
      end
      o = {}
      all_fragments.each do |f|
        base_object = f._named_columns['base_object']
        o['base_object'] = base_object if base_object
      end
      load_object_helper(all_fragments, o, 'properties', 'property_acls')
      o
    end

    def events rid, oid
      range = @obj_hist_version.get_range_prefix('realm_id' => rid, 'object_id' => oid)
      range.take(-1).collect{|r| r.row_name['data_event_id']}
    end

    private

    def get_fragments rid, oid, dei
      hist_version = @obj_hist_version.get_row('realm_id' => rid, 'object_id' => oid, 'data_event_id' => dei)
      if hist_version.nil?
        return nil
      end
      version = hist_version.columns['version']
      snapshot = version['snapshot'].dup
      snapshot['object_id'] = oid

      all_fragments = []
      all_fragments << @obj_fragment.get_row(snapshot)

      if version['diff_runs']
        version['diff_runs'].zip(version['diff_run_lengths']).each do |start, len|
          first = start.dup
          first['object_id'] = oid
          last = first.dup

          # there's an extra 1 here because get_range is exclusive
          last['fragment_version_id'] = first['fragment_version_id'] + len

          all_fragments += @obj_fragment.get_range(first, last).to_a
        end
      end

      all_fragments
    end

    # column is the db column name and the output key
    # acl_column defines the output key for the acls
    def load_object_helper all_fragments, o, column, acl_column
      o[column] = {}
      o[acl_column] = {}
      all_fragments.each do |f|
        c = f._named_columns[column]
        if c
          c['values'].each do |prop|
            o[column][prop['id']] = prop
          end

          c['instance_acls'].each do |acl|
            o[acl_column][acl['id']] = acl
          end
        end

        # remove deletes
        o[acl_column].reject! do |id, acl|
          acl['change'] == 'DELETE'
        end

        # remove unreferenced values
        acl_value_ids = o[acl_column].collect do |id, acl|
          acl['valueId']
        end
        o[column].reject! do |id, prop|
          !acl_value_ids.include?(id)
        end
      end
    end

    def method_missing method, *args, &block
      puts "Unknown method #{method} on #{self.class}"
      puts "Type 'help' for help with commands."
    end
  end

  class Streams
    BLOCK_SIZE_IN_BYTES = GenericStreamStore::BLOCK_SIZE_IN_BYTES

    def initialize connection
      @connection = connection
      @stream_metadata = connection.table 'stream_metadata'
      @stream_value = connection.table 'stream_value'
    end

    def get_metadata_row id
      @stream_metadata.get_row('id' => id)
    end

    def get_stream_as_hex id
      row = get_metadata_row(id)
      length = row.columns['metadata']['length']
      max_piece = (length - 1) / BLOCK_SIZE_IN_BYTES

      hex_string = ''
      (max_piece + 1).times do |i|
        hex_string << @stream_value.get_row('id' => id, 'block_id' => i).columns['value']
      end
      hex_string
    end

    def get_stream_raw id
      hex_string = get_stream_as_hex(id)
      hex_string_to_java_bytes(hex_string)
    end
  end

  #################
  ## Connections ##
  #################

  def prompt_connection_info
    info = {}

    types = AtlasShellConnectionType.values
    puts "Supported connection types: "
    for i in 1..types.size
      puts "#{i}: #{types[i - 1]}"
    end
    print "Connection type (1-#{types.size})? "
    type_index = gets.to_i - 1
    if type_index < 0 or type_index >= types.size
      $stderr.puts "Error parsing input"
      return
    end
    type = types[type_index]
    info[:type] = type.to_s

    if type.isIdentifierRequired
      print type.getIdentifierName + "? "
      info[:identifier] = gets.chomp.to_s
    end

    print "Host? "
    info[:host] = gets.chomp.to_s

    print "Port? "
    info[:port] = gets.chomp.to_s

    print "Username? "
    info[:username] = gets.chomp.to_s

    print "Password (warning: displayed in cleartext)? "
    info[:password] = gets.chomp.to_s

    return info
  end

  def load_dispatch_connection_info filename='dispatch.prefs'
    if !File.exist?(filename)
      $stderr.puts "Configuration file #{filename} could not be found."
      return nil
    end

    puts "WARNING: loading connection info from #{filename} will cause"
    puts "the DB password to be decrypted, displayed, and stored in the"
    print "clear.  Do you still wish to continue? (y/n) "
    confirmation = gets.chomp.to_s
    if confirmation[0].chr.downcase != 'y'
      puts "Nothing was loaded."
      return nil
    end

    info = {}
    prefs = PTConfiguration.new(java.io.File.new(filename))

    type = prefs.getString("DB_PREFS_TYPE")
    if type == "ORACLE"
      identifier = prefs.getString("DB_PREFS_SID")
    elsif type == "POSTGRESQL"
      identifier = prefs.getString("DB_PREFS_DBNAME")
    else
      $stderr.puts "Unrecognized DB type from prefs file: #{type}"
      return nil
    end

    # TODO (ejin): Support table splitting / Cassandra
    info[:type] = type
    info[:identifier] = identifier
    info[:host] = prefs.getString("DB_PREFS_HOST")
    info[:port] = prefs.getString("DB_PREFS_PORT")
    info[:username] = prefs.getString("DB_PREFS_USERNAME")
    info[:password] = SecureServer.decryptPreferenceValueWithoutPrompt(prefs, "DB_PREFS_PASSWORD")
    return info
  end

  # All the dirty global variable manipulation, which is hard to
  # unit-test, goes together at the very end so it is at least isolated
  def connect prefs=nil
    if prefs == nil
      prefs = prompt_connection_info
    end

    if prefs[:type].nil?
      $stderr.puts "Please provide a :type field in the connection information."
      return nil
    elsif prefs[:type] == "MEMORY"
      $db = Connection.snapshot_in_memory
    elsif prefs[:type] == "ROCKSDB"
        $db = Connection.snapshot_from_rocksdb(prefs[:host])
    elsif prefs[:type] == "CASSANDRA"
      missing_prefs = [:host,:port,:identifier] - prefs.keys
      if missing_prefs.empty?
        $db = Connection.readonly_from_cassandra(
          prefs[:host],
          prefs[:port].to_s,
          prefs[:identifier])
      else
        $stderr.puts "Please provide #{missing_prefs.inspect} in the connection information."
        return nil
      end
    else
      $stderr.puts "Invalid type: '#{prefs[:type]}' provided in the connection information."
      return nil
    end
    $db.table_names.each do |table_name|
      assign_table_variable table_name
    end
    if !$db.write_transactions_enabled
      puts "Connection to #{prefs[:type]} does not support write transactions; running in read-only mode."
    end
  rescue Exception => e
    $stderr.puts "AtlasDB Shell could not connect using the provided connection information.\nError: #{e.message}"
  end

  def assign_table_variable table_name
    begin
      table_name2 = table_name.gsub("-", "_")
      if table_name.include?(".")
        eval "$#{table_name2.split(".")[0]} ||= OpenStruct.new"
      else
        eval "$#{table_name2} = $db.table '#{table_name}'"
      end
    rescue SyntaxError
      puts "table #{table_name} can't be assigned to a ruby variable, access via $db.table '#{table_name}'"
    end
  end

  ##########################
  ## Timing / Prototyping ##
  ##########################

  def run_with_timer
    if !block_given?
      $stderr.puts "Error: must provide a code block to run."
      return
    end

    start_time = Time.now
    result = yield
    puts "Time used: #{Time.now - start_time} seconds"
    result
  end

  ###############
  ## Usability ##
  ###############

  def shell_help topic=:main
    [topic, '_toc'].each do |filename|
      dir = File.dirname(__FILE__)
      path_name = Pathname.new(dir).join("help", "#{filename}.txt").to_s
      if File.exists?(path_name)
        puts File.open(path_name, 'rb').read, "\n"
      else
        stream = java.lang.Object.java_class.resource_as_stream(path_name)
        if stream.nil?
          puts "The help page <#{filename}> does not exist."
          return nil
        else
          reader = java.io.BufferedReader.new(java.io.InputStreamReader.new(stream))
          while (line = reader.read_line) != nil
            puts line
          end
          reader.close
        end
      end
    end
    nil
  end

  def clear
    if $atlas_shell_gui_callback.isGraphicalViewEnabled
      $atlas_shell_gui_callback.clear
    elsif ENV['TERM']
      system('clear')
    else
      puts "The clear function is not supported in this environment."
    end
  end

  # Allows Ctrl+C to cancel 'each' operations
  IRB.conf[:IGNORE_SIGINT] = false
  Signal.trap("INT") { $atlas_shell_interrupt_callback.interrupt() }

  class << self
    # TODO (ejin): Suppress the annoying warning message about
    # overriding help when starting up the shell
    def help topic=:main
      shell_help topic
    end

    def method_missing method, *args, &block
      puts "Unknown AtlasDB shell command: #{method}"
      puts "Type 'help' for help with commands."
    end
  end

  def parse_hex_string str
    str = str.upcase
    bytes = hex_string_to_java_bytes(str)

    map = {}
    enum_classes = ValueType.values.to_a
    enum_classes.each do |enum_class|
      begin
        java_version = enum_class.convertToJava(bytes, 0)
        hex_string = java_to_hex_string(java_version, enum_class).upcase
        if str == hex_string
          map[enum_class.name] = java_version
        end
      rescue
        # ignore
      end
    end
    map
  end

  def java_to_hex_string java_obj, value_type_enum_class
    bytes = value_type_enum_class.convertFromJava(java_obj)
    bytes_to_hex_string(bytes)
  end

  def get_row_name_from_hex_string table, str
    bytes = hex_string_to_java_bytes(str)
    json = convert_row_name_bytes_to_json(table, bytes)
    decode_json(json)
  end

  def hex_string_to_java_bytes str
    str.chars.each_slice(2).collect do |sa|
      sa.join.to_i(16).chr
    end.join.to_java_bytes
  end

  def bytes_to_hex_string bytes
    bytes.collect do |b|
      "#{((b & 0xf0) >> 4).to_s(16)}#{(b & 0xf).to_s(16)}"
    end.join
  end

  def convert_row_name_bytes_to_json table, bytes
    table.metadata.getRowMetadata.renderToJson(bytes)
  end

  def decode_json json
    OkJson.decode(json)
  end

  def obj_to_ruby obj
    if obj.is_a?(java.util.HashMap)
      hash = {}
      obj.each do |k,v|
        hash[k] = obj_to_ruby(v)
      end
      hash
    elsif obj.is_a?(java.util.ArrayList)
      obj.to_a.collect do |elem|
        obj_to_ruby(elem)
      end
    elsif obj.is_a?(java.math.BigInteger)
      obj.to_s.to_i
    else
      obj
    end
  end

end

class << self
  # TODO (ejin): Suppress the annoying warning message about
  # overriding help when starting up the shell
  def help topic=:main
    AtlasDBShell.shell_help topic
  end

  def method_missing method, *args, &block
    puts "Unknown AtlasDB shell command: #{method}"
    puts "Type 'help' for help with commands."
  end
end

include AtlasDBShell

puts "Welcome to the AtlasDB shell.  Type 'help' for help with commands.\n\n"

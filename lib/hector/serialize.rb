class Hector
  module Serialize

    include_package 'me.prettyprint.cassandra.serializers'
    include_package 'me.prettyprint.cassandra.model'

    def serializers
      { :integer => IntegerSerializer.get,
        :string => StringSerializer.get,
        :long => LongSerializer.get,
        :bytes  => BytesArraySerializer.get,
        :infer => TypeInferringSerializer.get,
        :ascii => AsciiSerializer.get,
        :bigint => BigIntegerSerializer.get,
        :boolean => BooleanSerializer.get,
        :byte_buffer => ByteBufferSerializer.get,
        :char => CharSerializer.get,
        :date => DateSerializer.get,
        :double => DoubleSerializer.get,
        :float => FloatSerializer.get,
        :object => ObjectSerializer.get,
        :short => ShortSerializer.get,
        :uuid => UUIDSerializer.get}
    end

    def serializer(x)
      if x.kind_of?(Symbol)
        self.serializers[x]
      else
        SerializerTypeInferer.getSerializer(x)
      end
    end

    def h_to_rb(s)
      pp s
      case s
      when SuperRowsImpl
        s.inject(Hector::OrderedHash.new) {|acc, x| acc.merge(h_to_rb(x))}
      when SuperRowImpl
        {s.getKey => 
          s.getSuperSlice.getSuperColumns.inject(Hector::OrderedHash.new) {|acc, x| 
            acc.merge(h_to_rb(x)) }}
      when HSuperColumnImpl
        {s.getName => 
          s.getColumns.inject(Hector::OrderedHash.new) {|acc, x| 
            acc.merge(h_to_rb(x)) }}
      when RowsImpl
        pp "RowsImpl #{s}"
        s.inject(Hector::OrderedHash.new) {|acc, x| acc.merge(h_to_rb(x))}
      when RowImpl
        pp "RowImpl #{s}"
        {s.getKey => h_to_rb(s.getColumnSlice)}
      when ColumnSliceImpl
        s.getColumns.inject(Hector::OrderedHash.new) {|acc, x| acc.merge(h_to_rb(x))}
      when HColumnImpl
        {s.getName => s.getValue}
      when Fixnum
        {:count => s}
      when QueryResultImpl
        h_to_rb(s.get) # {:exec_us (.getExecutionTimeMicro s) # :host (.getHostUsed s)})
      when KeyspaceDefinition
        {s.getName => 
          {:replication_factor => s.getReplicationFactor,
            :strategy => s.getStrategyClass,
            :strategy_options => s.getStrategyOptions,
            :cf_defs => s.getCfDefs}}
      when ColumnFamilyDefinition
        {:name => s.getName,
         :comparator => parse_comparator(s.getComparatorType),
         :type => parse_column_type(s.getColumnType)}
      else
        raise "Unknown type #{s} (#{s.class}) for h_to_rb conversion"
      end
    end

    def parse_column_type(ct)
      case ct
        when ColumnType::SUPER then :super
        else :standard
      end
    end

    def parse_comparator(ct)
      case ct.getClassName
       when "org.apache.cassandra.db.marshal.UTF8Type"        then :utf8
       when "org.apache.cassandra.db.marshal.AsciiType"       then :ascii
       when "org.apache.cassandra.db.marshal.BytesType"       then :byte
       when "org.apache.cassandra.db.marshal.IntegerType"     then :integer
       when "org.apache.cassandra.db.marshal.LexicalUUIDType" then :lexical_uuid
       when "org.apache.cassandra.db.marshal.LongType"        then :long
       when "org.apache.cassandra.db.marshal.TimeUUIDType"    then :time_uuid
       else :unknown
      end
    end

  end
end
 

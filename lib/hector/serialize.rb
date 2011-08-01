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
      case s
      when SuperRowsImpl
        s.inject({}) {|acc, x| acc.merge(h_to_rb(x))}
      when SuperRowImpl
        {s.getKey => 
          s.getSuperSlice.getSuperColumns.inject({}) {|acc, x| 
            acc.merge(h_to_rb(x)) }}
      when HSuperColumnImpl
        {s.getName => 
          s.getColumns.inject({}) {|acc, x| 
            acc.merge(h_to_rb(x)) }}
      when RowsImpl
        s.inject({}) {|acc, x| acc.merge(h_to_rb(x))}
      when RowImpl
        {s.getKey => h_to_rb(s.getColumnSlice)}
      when ColumnSliceImpl
        s.getColumns.inject({}) {|acc, x| acc.merge(h_to_rb(x))}
      when HColumnImpl
        {s.getName => s.getValue}
      when Fixnum
        {:count => s}
      when QueryResultImpl
        h_to_rb(s.get) # {:exec_us (.getExecutionTimeMicro s) # :host (.getHostUsed s)})
      else
        raise "Unknown type #{s} (#{s.class}) for h_to_rb conversion"
      end
    end
  end
end
 

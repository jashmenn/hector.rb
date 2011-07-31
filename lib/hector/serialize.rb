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
      #pp [:h_to_rb, s, s.class, s.class == QueryResultImpl]
      case s
      #when SuperRowsImpl
        #(to-clojure [s]
        #            (map to-clojure (iterator-seq (.iterator s))))
      #when SuperRowImpl
        #(to-clojure [s]
        #            {(.getKey s) (map to-clojure (seq (.. s getSuperSlice getSuperColumns)))})
      #when HSuperColumnImpl
        #(to-clojure [s]
        #            {(.getName s) (into (hash-map) (for [c (.getColumns s)] (to-clojure c)))})
      when RowsImpl
        s.inject({}) {|acc, x| 
          #pp x
          acc.merge(h_to_rb(x)) 
        }
      when RowImpl
        {s.getKey => h_to_rb(s.getColumnSlice)}
      when ColumnSliceImpl
        s.getColumns.inject({}) {|acc, x| 
          #pp x
          acc.merge(h_to_rb(x)) 
        }
      when HColumnImpl
        ## pp [s.getNameSerializer, s.getValueSerializer]
        {s.getName => s.getValue}
        #(to-clojure [s]
        #            {(.getName s) (.getValue s)})
      # when Integer
        # {:count s}
        #(to-clojure [s]
        #            {:count s})
      when QueryResultImpl
        h_to_rb(s.get) # {:exec_us (.getExecutionTimeMicro s) #                                              :host (.getHostUsed s)})
      else
        pp ["NONE", s.class, s]
      end
    end
  end
end
 

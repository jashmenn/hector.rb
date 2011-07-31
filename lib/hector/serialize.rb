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
      pp [:h_to_rb, s, s.class, s.class == QueryResultImpl]
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
        s.inject({}) {|acc, x| acc.merge(h_to_rb(x)) }

        #(to-clojure [s]
        #            (map to-clojure (iterator-seq (.iterator s))))
      when RowImpl
        {s.getKey => h_to_rb(s.getColumnSlice)}
        #(to-clojure [s]
        #            {(.getKey s) (to-clojure (.getColumnSlice s))})
      when ColumnSliceImpl
        #s.getColumns.collect{|c| pp [:slice, c]; h_to_rb(c)}
        s.getColumns.inject({}) {|acc, x| acc.merge(h_to_rb(x)) }
        #(to-clojure [s]
        #            (into (hash-map) (for [c (.getColumns s)] (to-clojure c))))
      when HColumnImpl
        {s.getName => s.getValue}
        #(to-clojure [s]
        #            {(.getName s) (.getValue s)})
      #when Integer
        #(to-clojure [s]
        #            {:count s})
      when QueryResultImpl
        pp ["query reqult"]
        pp s.get
        h_to_rb(s.get) # {:exec_us (.getExecutionTimeMicro s) #                                              :host (.getHostUsed s)})
      else
        pp ["NONE", s.class, s]

      end
    end
  end
end
 

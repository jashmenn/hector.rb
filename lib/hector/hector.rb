import 'me.prettyprint.hector.api.factory.HFactory'
import 'me.prettyprint.hector.api.mutation.Mutator'
import 'me.prettyprint.hector.api.Cluster'
import 'me.prettyprint.hector.api.query.Query'
import 'me.prettyprint.cassandra.service.CassandraHostConfigurator'
import 'me.prettyprint.cassandra.serializers.TypeInferringSerializer'

=begin rdoc
=end

class Hector
  include Helpers
  include DDL

  class AccessError < StandardError #:nodoc:
  end

  WRITE_DEFAULTS = {
    :count => 1000,
    :timestamp => nil,
    #:consistency => Consistency::ONE,
    :ttl => nil
  }

  READ_DEFAULTS = {
    :count => 100,
    :start => nil,
    :finish => nil,
    #:consistency => Consistency::ONE,
    :reversed => false
  }

  SERIALIZATION_DEFAULTS = {
    :n_serializer => TypeInferringSerializer.get,
    :v_serializer => TypeInferringSerializer.get,
    :s_serializer => TypeInferringSerializer.get
  }


  attr_reader :keyspace, :cluster, :connection

  def self.cluster(cluster_name, server)
    HFactory.getOrCreateCluster(cluster_name, CassandraHostConfigurator.new(server))
  end

  # Create a new Hector instance and open the connection.
  def initialize(keyspace_name, server_or_cluster = "127.0.0.1:9160", options = {})
    cluster_name = options[:cluster_name] || "Hector"
    @cluster = server_or_cluster.kind_of?(String) ? self.class.cluster(cluster_name, server) : server_or_cluster
    self.keyspace = keyspace_name if keyspace_name
  end

  def keyspace=(keyspace_name)
    @keyspace = HFactory.createKeyspace(keyspace_name, @cluster)
  end

  def disconnect
    HFactory.shutdownCluster(@cluster);
  end

  def type_inferring
    TypeInferringSerializer.get
  end

  def create_column(n, v, opts={})
    opts = SERIALIZATION_DEFAULTS.merge(opts)
    pp [n, v, opts]
    if v.kind_of?(Hash)
      cols = v.collect {|name,value| create_column(name, value, opts)}
      HFactory.createSuperColumn(n, cols, opts[:s_serializer], opts[:n_serializer], opts[:v_serializer])
      # todo
    #(let [cols (map (fn [[n v]] (create-column n v :n-serializer n-serializer :v-serializer v-serializer)) v)]
    #  (HFactory/createSuperColumn n cols s-serializer n-serializer v-serializer))
    else
      col = HFactory.createColumn(n, v, opts[:n_serializer], opts[:v_serializer])
      #pp col
      col
    end
  end

  ##
  # This is the main method used to insert rows into cassandra. If the
  # column\_family that you are inserting into is a SuperColumnFamily then
  # the hash passed in should be a nested hash, otherwise it should be a
  # flat hash.
  #
  # This method can also be called while in batch mode. If in batch mode
  # then we queue up the mutations (an insert in this case) and pass them to
  # cassandra in a single batch at the end of the block.
  #
  # * column\_family - The column\_family that you are inserting into.
  # * key - The row key to insert.
  # * hash - The columns or super columns to insert.
  # * options - Valid options are:
  #   * :timestamp - Uses the current time if none specified.
  #   * :consistency - Uses the default write consistency if none specified.
  #   * :ttl - If specified this is the number of seconds after the insert that this value will be available.
  #
  def insert(column_family, key, hash, options = {})
    column_family = column_family.to_s
    #column_family, _, _, options = extract_and_validate_params(column_family, key, [options], WRITE_DEFAULTS)

    options = WRITE_DEFAULTS.merge(options)

    #pp [column_family, options]

    type_inferring = nil
    mut = HFactory.createMutator(@keyspace, self.type_inferring)
    hash.each do |k,v|
      mut.addInsertion(key, column_family, create_column(k, v, options))
    end

    mut.execute

    #timestamp = options[:timestamp] || Time.stamp
    #mutation_map = if false #is_super(column_family)
    #  {
    #    key => {
    #      column_family => "a" #hash.collect{|k,v| _super_insert_mutation(column_family, k, v, timestamp, options[:ttl]) }
    #    }
    #  }
    #else
    #  {
    #    key => {
    #      column_family => "b" #hash.collect{|k,v| _standard_insert_mutation(column_family, k, v, timestamp, options[:ttl])}
    #    }
    #  }
    #end
    # batch TODO
    # @batch ? @batch << [mutation_map, options[:consistency]] : _mutate(mutation_map, options[:consistency])
    #pp mutation_map
    #_mutate(mutation_map, options[:consistency])
  end
end

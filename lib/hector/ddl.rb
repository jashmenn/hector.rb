import 'me.prettyprint.hector.api.factory.HFactory'
import 'me.prettyprint.hector.api.Cluster'
import 'me.prettyprint.cassandra.service.ThriftCfDef'
import 'me.prettyprint.hector.api.ddl.ComparatorType'
import 'me.prettyprint.hector.api.ddl.ColumnFamilyDefinition'
import 'me.prettyprint.hector.api.ddl.ColumnType'
import 'me.prettyprint.hector.api.ddl.KeyspaceDefinition'

class Hector
  module DDL
    def add_keyspace(ks_def)
      strategy = case ks_def[:strategy]
                   when :local;            "org.apache.cassandra.locator.LocalStrategy"
                   when :network_topology; "org.apache.cassandra.locator.NetworkTopologyStrategy"
                 else "org.apache.cassandra.locator.SimpleStrategy"
                 end
      replication = ks_def[:replication] || 1
      @cluster.addKeyspace(make_keyspace_definition(ks_def[:name], strategy, replication, ks_def[:column_families]))
    end

    def make_column_family(keyspace, cf_def)
      name, comparator_type, column_type = cf_def[:name], cf_def[:comparator], cf_def[:type]
      hcf = if comparator_type
             comparator = if comparator_type.class == Class
                            comparator_type
                          else
                            case comparator_type
                            when :ascii         then ComparatorType.ASCIITYPE
                            when :byte          then ComparatorType.BYTESTYPE
                            when :integer       then ComparatorType.INTEGERTYPE
                            when :lexical_uuid  then ComparatorType.LEXICALUUIDTYPE
                            when :long          then ComparatorType.LONGTYPE
                            when :time_uuid     then ComparatorType.TIMEUUIDTYPE
                            when :utf8          then ComparatorType.UTF8TYPE
                            else raise "Unknown comparator type passed in column family definition"
                            end
                          end
             HFactory.createColumnFamilyDefinition(keyspace, name, comparator)
           else
             HFactory.createColumnFamilyDefinition(keyspace, name)
           end
      if column_type
        hcf.setColumnType( column_type == :super ? ColumnType.SUPER : ColumnType.STANDRD )
      end
      hcf
    end

    def make_keyspace_definition(keyspace, strategy, replication, cfs)
      column_families = cfs.collect{|cf| make_column_family(keyspace, cf) } 
      HFactory.createKeyspaceDefinition(keyspace, strategy, replication, column_families)
    end

    def drop_keyspace(keyspace)
      @cluster.dropKeyspace keyspace
    end

  end
end



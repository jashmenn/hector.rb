require 'spec_helper'
describe "HectorDDL" do

  def setup_keyspace_and_client(column_families)
    @cluster = Hector.cluster("Hector", "127.0.0.1:9160")
    @ks_name = java.util.UUID.randomUUID.to_s.gsub("-","")
    @client = Hector.new(nil, @cluster, :retries => 2, :exception_classes => [])
    @client.add_keyspace({:name => @ks_name, :strategy => :local, :replication => 1, :column_families => column_families}) 
    @client.keyspace = @ks_name
  end

  def teardown_keyspace_and_client
    @client.drop_keyspace(@ks_name)
    @client.disconnect
  end

  after(:each) do
    teardown_keyspace_and_client
  end

  context "ColumnFamily 'a' (Standard with String comparator)" do
    before(:each) do
      setup_keyspace_and_client([{:name => @cf="a"}])
    end

    it "should describe keyspaces" do
      ks = @client.keyspaces
      ks.should include("system")
      ks[@ks_name][:replication_factor].should eq(1)
    end

    pending "should make column families"
    pending "should describe column families"
    pending "should drop column families"

  end

end

 

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

  context "with Standard ColumnFamily" do
    before(:each) do
      setup_keyspace_and_client([{:name => @cf="a"}])
    end

    it "should describe keyspaces" do
      ks = @client.describe_keyspaces
      ks.should include("system")
      ks[@ks_name][:replication_factor].should eq(1)
    end
 end

  context "with Standard ColumnFamilies" do
    before(:each) do
      setup_keyspace_and_client([{:name => "a"},
                                 {:name => "b", :comparator => :long}])
    end

    it "should describe column families" do
      cf = @client.column_families
      cf["a"].should eq({:name => "a", :comparator => :byte, :type => :standard})
      cf["b"].should eq({:name => "b", :comparator => :long, :type => :standard})
    end

    it "should add and drop column families" do
      # ensure "c" doesn't exist
      cf = @client.column_families
      cf["c"].should be_nil

      # add "c"
      @client.add_column_family({:name => "c", :comparator => :long})
      cf = @client.column_families
      cf["c"].should eq({:name => "c", :comparator => :long, :type => :standard})

      # drop "c"
      @client.drop_column_family("c")

      # ensure "c" doesn't exist
      cf = @client.column_families
      cf["c"].should be_nil
    end


    it "should add and drop super column families" do
      # ensure "c" doesn't exist
      cf = @client.column_families
      cf["c"].should be_nil

      # add "c"
      @client.add_column_family({:name => "c", :comparator => :long, :type => :super})
      cf = @client.column_families
      cf["c"].should eq({:name => "c", :comparator => :long, :type => :super})

      # drop "c"
      @client.drop_column_family("c")

      # ensure "c" doesn't exist
      cf = @client.column_families
      cf["c"].should be_nil
    end

  end


end

 

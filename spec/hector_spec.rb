require 'spec_helper'
describe Hector do

  def setup_keyspace_and_client(column_families)
    @cluster = Hector.cluster("Hector", "127.0.0.1:9160")
    @ks_name = java.util.UUID.randomUUID.to_s.gsub("-","")
    @client = Hector.new(nil, @cluster, :retries => 2, :exception_classes => [])
    @client.add_keyspace({:name => @ks_name, :strategy => :local, :replication => 1, :column_families => column_families}) 
    @client.keyspace = @ks_name
  end

  def shutdown
    @client.drop_keyspace(@ks_name)
    @client.disconnect
  end

  after(:each) do
    shutdown
  end

  context "ColumnFamily 'a' (Standard with String comparator)" do

    before(:each) do
      setup_keyspace_and_client([{:name => @cf="a"}])
    end

    context "with a string key & string value" do
      before(:each) do
        @opts = {:n_serializer => :string, :v_serializer => :string, :s_serializer => :string}
        @client.put_row(@cf, "row-key", {"k" => "v"})
      end

      it "should get entire rows" do
        @client.get_rows(@cf, ["row-key"], @opts).should eq( {"row-key" => {'k' => 'v'}} )
      end

      it "should get individual columns" do
        @client.get_columns(@cf, "row-key", ["k"], @opts).should eq( {'k' => 'v'} )
      end
      pending "should get multiple columns with string key attribute"

      it "should be empty if we've deleted the column" do
        @client.delete_columns(@cf, "row-key", ["k"])
        @client.get_rows(@cf, ["row-key"], @opts).should eq( {"row-key" => {}} )
      end
    end

    context "with a string key & long value" do
      before(:each) do
        @opts = {:n_serializer => :string, :v_serializer => :long, :s_serializer => :string}
        @client.put_row(@cf, "row-key", {"k" => 1234})
      end

      it "should get entire rows" do
        @client.get_rows(@cf, ["row-key"], @opts).should eq( {"row-key" => {'k' => 1234}} )
      end

      it "should get individual columns" do
        @client.get_columns(@cf, "row-key", ["k"], @opts).should eq( {'k' => 1234} )
      end
    end

    context "with a long key & long value" do
      before(:each) do
        @opts = {:n_serializer => :long, :v_serializer => :long, :s_serializer => :string}
        @client.put_row(@cf, "row-key", {1 => 1234})
      end

      it "should get entire rows" do
        @client.get_rows(@cf, ["row-key"], @opts).should eq( {"row-key" => {1 => 1234}} )
      end

      it "should get individual columns" do
        @client.get_columns(@cf, "row-key", [1], @opts).should eq( {1 => 1234} )
      end
    end

    context "defaults to byte array for name value serialization" do
      before(:each) do
        @opts = {:n_serializer => :bytes, :v_serializer => :bytes}
        @client.put_row(@cf, "row-key", {"k" => "v"})
      end

      it "should get proper byte[]" do
        row = (@client.get_rows(@cf, ["row-key"], @opts))["row-key"]
        n_bytes = row.to_a.first.first
        v_bytes = row.to_a.first.last
        java.lang.String.new(n_bytes).should eq( "k" )
        java.lang.String.new(v_bytes).should eq( "v" )
      end

      it "should get byte[] columns with bytes key" do
        row = @client.get_columns(@cf, "row-key", [java.lang.String.new("k").getBytes], @opts)
        n_bytes = row.to_a.first.first
        v_bytes = row.to_a.first.last
        java.lang.String.new(n_bytes).should eq( "k" )
        java.lang.String.new(v_bytes).should eq( "v" )
      end

      it "should get byte[] columns with bytes key" do
        opts = @opts.merge({:n_serializer => :string})
        row = @client.get_columns(@cf, "row-key", ["k"], opts)
        n       = row.to_a.first.first
        v_bytes = row.to_a.first.last
                                    n.should eq( "k" )
        java.lang.String.new(v_bytes).should eq( "v" )
      end
    end

    context "with a couple of columns" do
      before(:each) do
        @client.put_row(@cf, "row-key", {"k" => "v", "k2" => "v2"})
      end

      it "should count properly" do
        @client.count_columns(@cf, "row-key").should eq({:count => 2})
      end
    end
  end

  context "ColumnFamily 'b' (Standard with Long comparator)" do

    before(:each) do
      setup_keyspace_and_client([{:name => @cf="b", :comparator => :long}])
    end

    context "with a long key & long value" do
      before(:each) do
        @opts = {:n_serializer => :long, :v_serializer => :long}
        @client.put_row(@cf, 101, {1 => 1234})
      end

      it "should get entire rows" do
        @client.get_rows(@cf, [101], @opts).should eq( {101 => {1 => 1234}} )
      end

      it "should get individual columns" do
        @client.get_columns(@cf, 101, [1], @opts).should eq( {1 => 1234} )
      end
    end

    context "with several long keys & long values" do
      before(:each) do
        @opts = {:n_serializer => :long, :v_serializer => :long}
        @client.put_row(@cf, "row-key", 
                        { 1 => 101,
                          2 => 102,
                          3 => 103,
                          4 => 104 })
      end

      it "should get several rows" do
        opts = @opts.merge({:start => 2, :finish => 3})
        @client.get_rows(@cf, ["row-key"], opts).should eq( {"row-key" => {2 => 102, 3 => 103}} )
      end

      pending "should use ordered hashes"
    end
  end

  context "ColumnFamily 'c' SuperColumn" do
    before(:each) do
      setup_keyspace_and_client([{:name => @cf="c", :type => :super}])
    end

    context "with a string key & string value" do
      before(:each) do
        @opts = {:n_serializer => :string, :v_serializer => :string, :s_serializer => :string}
        @client.put_row(@cf, "row-key", 
                        { "SuperCol"  => {"k" => "v", "k2" => "v2"},
                          "SuperCol2" => {"k" => "v", "k2" => "v2"} })
      end

      pending "should be able to detect a super column" do
        # :super 
      end

      it "should get super rows" do
        @client.get_super_rows(@cf, ["row-key"], ["SuperCol", "SuperCol2"], @opts).first.should 
           eq( {"row-key" => [{"SuperCol"  => {"k" => "v", "k2" => "v2"}},
                              {"SuperCol2" => {"k" => "v", "k2" => "v2"}}]} )
      end

      it "should get super columns" do
        @client.get_super_columns(@cf, "row-key", "SuperCol", ["k2", "v2"], @opts).should 
          eq( {"k" => "v", "k2" => "v2"} )
      end
    end

  end


# (deftest supercolumn-with-string-key-name-and-value
#   (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
#         cf "a"
#         ks (keyspace *test-cluster* ks-name)
#         opts [:v-serializer :string
#               :n-serializer :string
#               :s-serializer :string]]
#     (ddl/add-keyspace *test-cluster* {:name ks-name
#                                       :strategy :local
#                                       :replication 1
#                                       :column-families [{:name cf
#                                                          :type :super}]})
#     (put-row ks cf "row-key" {"SuperCol" {"k" "v"
#                                           "k2" "v2"}
#                               "SuperCol2" {"k" "v"
#                                            "k2" "v2"}})
#     (is (= :super
#            (:type (first (ddl/column-families *test-cluster* ks-name)))))
#     (is (= {"row-key" [{"SuperCol" {"k" "v"
#                                     "k2" "v2"}}
#                        {"SuperCol2" {"k" "v"
#                                      "k2" "v2"}}]} 
#            (first (apply get-super-rows ks cf ["row-key"] ["SuperCol" "SuperCol2"] opts))))
#     (is (= {"k2" "v2"}
#            (apply get-super-columns ks cf "row-key" "SuperCol" ["k2" "v2"] opts)))
#     (ddl/drop-keyspace *test-cluster* ks-name)))

# (deftest deleting-supercolumns
#   (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
#         cf "a"
#         ks (keyspace *test-cluster* ks-name)
#         opts [:v-serializer :string
#               :n-serializer :string
#               :s-serializer :string]]
#     (ddl/add-keyspace *test-cluster* {:name ks-name
#                                       :strategy :local
#                                       :replication 1
#                                       :column-families [{:name cf
#                                                          :type :super}]})
#     (put-row ks cf "row-key" {"SuperCol" {"k" "v"
#                                           "k2" "v2"}
#                               "SuperCol2" {"k" "v"
#                                            "k2" "v2"}})
#     (is (= {"k2" "v2"
#             "k" "v"}
#            (apply get-super-columns ks cf "row-key" "SuperCol" ["k" "k2"] opts)))
#     (apply delete-super-columns ks cf {"row-key" {"SuperCol" ["k2"] "SuperCol2" ["k2"]}} opts)
#     (is (= {"k" "v"}
#            (apply get-super-columns ks cf "row-key" "SuperCol" ["k" "k2"] opts)))
#     (is (= {"k" "v"}
#            (apply get-super-columns ks cf "row-key" "SuperCol2" ["k" "k2"] opts)))
#     (ddl/drop-keyspace *test-cluster* ks-name)))
  

end

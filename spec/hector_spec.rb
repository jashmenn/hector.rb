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

  context "with a string key & string value" do
    before(:each) do
      setup_keyspace_and_client([{:name => @cf="a"}])
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
      setup_keyspace_and_client([{:name => @cf="a"}])
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
      setup_keyspace_and_client([{:name => @cf="a"}])
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


# (deftest long-key-long-name-and-values
#   (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
#         cf "a"
#         ks (keyspace *test-cluster* ks-name)
#         opts [:n-serializer :long
#               :v-serializer :long]]
#     (ddl/add-keyspace *test-cluster* {:name ks-name
#                                       :strategy :local
#                                       :replication 1
#                                       :column-families [{:name cf
#                                                          :comparator :long}]})
#     (put-row ks cf (long 101) {(long 1) (long 1234)})
#     (is (= {(long 101) {(long 1)(long 1234)}}
#            (first (apply get-rows ks cf [(long 101)] opts))))
#     (is (= {(long 1) (long 1234)}
#            (apply get-columns ks cf (long 101) [(long 1)] opts)))
#     (ddl/drop-keyspace *test-cluster* ks-name)))


# (deftest string-key-long-name-and-values-with-range
#   (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
#         cf "a"
#         ks (keyspace *test-cluster* ks-name)]
#     (ddl/add-keyspace *test-cluster* {:name ks-name
#                                       :strategy :local
#                                       :replication 1
#                                       :column-families [{:name cf
#                                                          :comparator :long}]})
#     (put-row ks cf "row-key" {(long 1) (long 101)
#                               (long 2) (long 102)
#                               (long 3) (long 103)
#                               (long 4) (long 104)})
#     (is (= {"row-key" (sorted-map (long 2) (long 102)
#                                  (long 3) (long 103))}
#            (first (apply get-rows ks cf ["row-key"] [:n-serializer :long
#                                                      :v-serializer :long
#                                                      :start (long 2)
#                                                      :end (long 3)]))))
#     (ddl/drop-keyspace *test-cluster* ks-name)))

# (deftest defaults-to-byte-array-for-name-value-serialization
#   (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
#         cf "a"
#         ks (keyspace *test-cluster* ks-name)]
#     (ddl/add-keyspace *test-cluster* {:name ks-name
#                                       :strategy :local
#                                       :replication 1
#                                       :column-families [{:name cf}]})
#     (put-row ks cf "row-key" {"k" "v"})
#     (let [first-row (get (first (get-rows ks cf ["row-key"])) "row-key")
#           n-bytes (first (keys first-row))
#           v-bytes (first (vals first-row))]
#       (is (= "k"
#              (String. n-bytes)))
#       (is (= "v"
#              (String. v-bytes))))
#     (let [res (get-columns ks cf "row-key" [(.getBytes "k")])
#           n-bytes (first (keys res))
#           v-bytes (first (vals res))]
#       (is (= "k"
#              (String. n-bytes)))
#       (is (= "v"
#              (String. v-bytes))))
#     (let [res (apply get-columns ks cf "row-key" ["k"] [:n-serializer :string])
#           n (first (keys res))
#           v-bytes (first (vals res))]
#       (is (= "k" n))
#       (is (= "v"
#              (String. v-bytes))))
#     (ddl/drop-keyspace *test-cluster* ks-name)))

# (deftest counting
#   (let [ks-name (.replace (str "ks" (java.util.UUID/randomUUID)) "-" "")
#         cf "a"
#         ks (keyspace *test-cluster* ks-name)]
#     (ddl/add-keyspace *test-cluster* {:name ks-name
#                                       :strategy :local
#                                       :replication 1
#                                       :column-families [{:name cf}]})
#     (put-row ks cf "row-key" {"k" "v" "k2" "v2"})
#     (is (= {:count 2}
#            (count-columns ks "row-key" cf)))
#     (ddl/drop-keyspace *test-cluster* ks-name)))

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

root = File.expand_path(File.dirname(__FILE__) + "/../")
$:.unshift(root + "/lib")
require 'rubygems'
require 'java'
require 'hector'
require 'simple_uuid'

# gem install cassandra (not a dependency, just an easy way to setup cassandra)
# cassandra_helper cassandra
# cassandra_helper data:load

twitter = Hector.new('Twitter')
#user = {'screen_name' => 'buttonscat'}
#twitter.insert(:Users, '5', user)
#pp twitter.get(:Users, '5')

#tweet1 = {'text' => 'Nom nom nom nom nom.', 'user_id' => '5'}
#twitter.insert(:Statuses, '1', tweet1)

#tweet2 = {'text' => '@evan Zzzz....', 'user_id' => '5', 'reply_to_id' => '8'}
#twitter.insert(:Statuses, '2', tweet2)
#twitter.insert(:UserRelationships, '5', {'user_timeline' => {SimpleUUID::UUID.new => '1'}})
#twitter.insert(:UserRelationships, '5', {'user_timeline' => {UUID.new => '2'}})

#timeline = twitter.get(:UserRelationships, '5', 'user_timeline', :reversed => true)
#timeline.map { |time, id| twitter.get(:Statuses, id, 'text') }
# => ["@evan Zzzz....", "Nom nom nom nom nom."]

# insert
# remove
# get
# multi_get
# get_range

twitter.shutdown # hector hangs otherwise
puts "done"

$:.unshift(File.expand_path(File.dirname(__FILE__))+ "/../lib")
require 'rubygems'
require "hector"
require 'bundler/setup'

RSpec.configure do |config|
  begin
    @test_client = Hector.new('Twitter', 'localhost:9160', {:exception_classes => []})
  rescue Exception => e
    #FIXME Make server automatically start if not running
    if e.message =~ /Could not connect/
      puts "*** Please start the Cassandra server by running 'rake cassandra'. ***"
      exit 1
    end
  end
end

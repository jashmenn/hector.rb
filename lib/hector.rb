require 'rubygems'
require 'pp'
require 'java'
require "hector/version"

gem 'simple_uuid' , '~> 0.1.0'
require 'simple_uuid'

here = File.expand_path(File.dirname(__FILE__))

class Hector ; end

jars_dir = File.dirname(__FILE__) + "/../vendor/jars"
$LOAD_PATH << jars_dir

Dir.entries(jars_dir).sort.each do |entry|
  if entry =~ /.jar$/
    require entry
  end
end

$LOAD_PATH << "#{here}"

require 'hector/helpers'
require 'hector/time'
require "hector/hector"

require 'rubygems'
require 'pp'
require 'java'

here = File.expand_path(File.dirname(__FILE__))
$LOAD_PATH << "#{here}"

class Hector ; end

jars_dir = File.dirname(__FILE__) + "/../vendor/jars"
$LOAD_PATH << jars_dir

Dir.entries(jars_dir).sort.each do |entry|
  if entry =~ /.jar$/
    require entry
  end
end

require "hector/version"
require 'hector/helpers'
require 'hector/time'
require "hector/hector"

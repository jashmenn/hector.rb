require 'rubygems'
require 'java'
require "hector/version"

here = File.expand_path(File.dirname(__FILE__))

module Hector ; end

jars_dir = File.dirname(__FILE__) + "/../vendor/jars"
$LOAD_PATH << jars_dir

Dir.entries(jars_dir).sort.each do |entry|
  if entry =~ /.jar$/
    puts entry
    require entry
  end
end

$LOAD_PATH << "#{here}"

require "hector/hector"

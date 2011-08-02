# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "hector/version"

Gem::Specification.new do |s|
  s.name        = "hector"
  s.version     = Hector::VERSION
  s.authors     = ["Nate Murray"]
  s.email       = ["nate@natemurray.com"]
  s.homepage    = "http://www.xcombinator.com/"
  s.summary     = %q{A Cassandra client for JRuby based on Hector}
  s.description = %q{A Cassandra client for JRuby based on Hector.}

  s.rubyforge_project = "hector.rb"

  s.files         = `git ls-files`.split("\n") + `find vendor/jars -type f -name *.jar`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end

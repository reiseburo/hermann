
$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + '/lib'))

require 'hermann/version'

SPEC = Gem::Specification.new do |s|
  s.name               = "hermann"
  s.version            = Hermann::VERSION

  s.authors = ["Stan Campbell", 'R. Tyler Croy']
  s.description = 'Ruby gem wrapper for librdkafka'
  s.email = ['stan.campbell3@gmail.com', 'rtyler.croy@lookout.com']
  s.files = [ "Rakefile", "ext/hermann_lib.h", "ext/hermann_lib.c", "ext/extconf.rb"]
  s.files += `git ls-files -- lib`.split($\)
  s.extensions = [ "ext/extconf.rb"]
  s.homepage = 'https://github.com/lookout/Hermann'
  s.require_paths = ["lib", "ext"]
  s.rubygems_version = '2.2.2'
  s.summary = 'A Kafka consumer/producer gem based on the librdkafka C library.'
  s.licenses = ['MIT']

  s.specification_version = 3 if s.respond_to?(:specification_version)
end

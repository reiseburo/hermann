
$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + '/lib'))

require 'hermann/version'

SPEC = Gem::Specification.new do |s|
  s.name               = "hermann"
  s.version            = Hermann::VERSION

  s.authors = ["Stan Campbell", 'R. Tyler Croy']
  s.description = 'Ruby gem wrapper for librdkafka'
  s.summary = 'A Kafka consumer/producer gem based on the librdkafka C library.'
  s.email = ['stan.campbell3@gmail.com', 'rtyler.croy@lookout.com']
  s.homepage = 'https://github.com/lookout/Hermann'
  s.licenses = ['MIT']

  s.files = [ "Rakefile"]
  s.files += `git ls-files -- lib`.split($\)
  s.files += `git ls-files -- ext`.split($\)

  s.extensions = Dir['ext/**/extconf.rb']

  s.require_paths = ["lib", "ext/hermann"]
  s.rubygems_version = '2.2.2'

  s.specification_version = 3 if s.respond_to?(:specification_version)
end

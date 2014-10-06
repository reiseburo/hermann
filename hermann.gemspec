
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


  s.rubygems_version = '2.2.2'
  s.specification_version = 3 if s.respond_to?(:specification_version)

  s.add_dependency 'concurrent-ruby'

  if RUBY_PLATFORM == "java"
    s.add_runtime_dependency 'jar-dependencies', '~>0.1.2'
    s.add_development_dependency 'ruby-maven', '~> 3.1.1.0'
    s.add_development_dependency 'rake'
    s.requirements << "jar org.apache.kafka:kafka_2.10, 0.8.1.1"
    s.requirements << "jar org.mod4j.org.eclipse.xtext:log4j, 1.2.15"
    s.requirements << "jar org.scala-lang:scala-library, 2.10.1"
    s.requirements << "jar com.yammer.metrics:metrics-core, 2.2.0"
    s.requirements << "jar org.slf4j:slf4j-api, 1.7.2"
    s.requirements << "jar com.101tec:zkclient, 0.3"
    s.require_paths = ["lib"]
  else
    s.add_dependency('mini_portile', '~> 0.6.0')
    s.extensions = Dir['ext/**/extconf.rb']
    s.require_paths = ["lib", "ext/hermann"]
  end
end

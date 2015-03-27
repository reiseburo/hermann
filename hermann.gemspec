
$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + '/lib'))

require 'hermann/version'


Gem::Specification.new do |s|
  s.name               = "hermann"
  s.version            = Hermann::VERSION

  s.authors = ['R. Tyler Croy', 'James Way', "Stan Campbell"]
  s.description = 'Ruby gem for talking to Kafka'
  s.summary = 'A Kafka consumer/producer gem supporting both MRI and JRuby'
  s.email = ['rtyler.croy@lookout.com', 'james.way@lookout.com', 'stan.campbell3@gmail.com']
  s.homepage = 'https://github.com/lookout/Hermann'
  s.licenses = ['MIT']

  s.files = [ "Rakefile"]
  s.files += `git ls-files -- lib`.split($\)
  s.files += `git ls-files -- ext`.split($\)


  s.rubygems_version = '2.2.2'
  s.specification_version = 3 if s.respond_to?(:specification_version)

  s.add_dependency 'concurrent-ruby', '~> 0.7.0'
  s.add_dependency 'thread_safe', '~> 0.3.4'

  if RUBY_PLATFORM == "java"
    # IMPORTANT: make sure that jar-dependencies is only a development
    # dependency of your gem. then all jars will be vendored inside the
    # gem during installation.
    s.add_dependency 'jar-dependencies', '~>0.1.9'
    # exclude junit:junit
    s.requirements << "jar org.apache.kafka:kafka_2.10, ~>0.8.1.1, ['junit:junit']"
    s.requirements << "jar org.apache.curator:curator-framework, ~>2.7"
    s.requirements << "jar log4j:log4j, ~>1.2"
    s.require_paths = ["lib"]
    s.platform = 'java'
  else
    s.add_dependency('mini_portile', '~> 0.6.0')
    s.extensions = Dir['ext/**/extconf.rb']
    s.require_paths = ["lib", "ext/hermann"]
  end
end

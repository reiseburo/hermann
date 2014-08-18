SPEC = Gem::Specification.new do |s|
  s.name               = "hermann"
  s.version            = "0.11"
  s.default_executable = "hermann"

  s.authors = ["Stan Campbell"]
  s.date = %q{2014-05-29}
  s.description = %q{Ruby gem wrapper for the RdKafka C library}
  s.email = %q{stan.campbell3@gmail.com}
  s.files = [ "Rakefile", "ext/hermann_lib.h", "ext/hermann_lib.c", "ext/extconf.rb", "bin/hermann",
              "lib/hermann.rb", "ext/hermann_lib.o"]
  s.extensions = [ "ext/extconf.rb"]
  s.test_files = ["test/test_hermann.rb"]
  s.homepage = %q{http://https://github.com/lookout/Hermann}
  s.require_paths = ["lib", "ext"]
  s.rubygems_version = %q{2.2.2}
  s.summary = %q{The Kafka consumer is based on the librdkafka C library.}

  s.platform = Gem::Platform::CURRENT

  s.specification_version = 3 if s.respond_to?(:specification_version)
end

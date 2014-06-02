
SPEC = Gem::Specification.new do |s|
  s.name               = "hermann"
  s.version            = "0.0.31"
  s.default_executable = "hermann"

  s.authors = ["Stan Campbell"]
  s.date = %q{2014-05-29}
  s.description = %q{Ruby gem wrapper for a C based Kafka Consumer}
  s.email = %q{stan.campbell3@gmail.com}
  s.files = [ "Rakefile", "ext/hermann_lib.h", "ext/hermann_lib.c", "ext/extconf.rb", "bin/hermann",
              "lib/hermann.rb", "ext/hermann_lib.o"]
  s.extensions = [ "ext/extconf.rb"]
  s.test_files = ["test/test_hermann.rb"]
  s.homepage = %q{http://rubygems.org/gems/hermann}
  s.require_paths = ["lib", "ext"]
  s.rubygems_version = %q{2.2.2}
  s.summary = %q{The Kafka consumer is based on the librdkafka C library.}

  s.platform = Gem::Platform::CURRENT

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
    else
    end
  else
  end
end
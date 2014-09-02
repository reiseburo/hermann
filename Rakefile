require 'rubygems'
require "bundler/gem_tasks"
require 'rspec/core/rake_task'
require 'rake/extensiontask'


Rake::ExtensionTask.new do |t|
  t.name = 'hermann_lib'
  t.ext_dir = 'ext/hermann'
  t.gem_spec = Gem::Specification.load('hermann.gemspec')
end

RSpec::Core::RakeTask.new(:spec)

task :build => [:compile]
task :default => [:clean, :build, :spec]


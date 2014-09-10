require 'rubygems'
require "bundler/gem_tasks"
require 'rspec/core/rake_task'
require 'rake/extensiontask'


Rake::ExtensionTask.new do |t|
  t.name = 'hermann_lib'
  t.ext_dir = 'ext/hermann'
  t.gem_spec = Gem::Specification.load('hermann.gemspec')
end

RSpec::Core::RakeTask.new(:spec) do |r|
  r.rspec_opts = '--tag ~type:integration'
end

namespace :spec do
  RSpec::Core::RakeTask.new(:integration) do |r|
    r.rspec_opts = '--tag type:integration'
  end
end

task :build => [:compile]
task :default => [:clean, :build, :spec]


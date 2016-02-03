require 'rubygems'
require 'fileutils'
require "bundler/gem_tasks"
require 'rspec/core/rake_task'
require 'rake/extensiontask'
require 'ci/reporter/rake/rspec'

Rake::ExtensionTask.new do |t|
  t.name = 'hermann_rdkafka'
  t.ext_dir = 'ext/hermann'
  t.gem_spec = Gem::Specification.load('hermann.gemspec')
end

def add_rspec_options(options)
  if RUBY_PLATFORM == 'java'
    options << '--tag ~platform:mri'
  else
    options << '--tag ~platform:java'
  end
  return options
end

RSpec::Core::RakeTask.new(:spec) do |r|
  options = add_rspec_options(['--tag ~type:integration'])

  r.rspec_opts = options.join(' ')
end

namespace :spec do
  RSpec::Core::RakeTask.new(:integration) do |r|
    options = add_rspec_options(['--tag type:integration'])
    r.rspec_opts = options.join(' ')
  end
end

desc 'Remove the entire ./tmp directory'
task :removetmp do
  FileUtils.rm_rf('tmp')
end

task :clean => [:removetmp]

if RUBY_PLATFORM == 'java'
  task :default => [:clean, :spec]
else
  task :build => [:compile]
  task :default => [:clean, :build, :spec]
end


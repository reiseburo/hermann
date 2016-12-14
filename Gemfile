source "https://rubygems.org"

gemspec

group :development do
  gem 'jbundler', :platform => :jruby
  gem 'rake', '~> 11.3.0'
  gem 'i18n', '~> 0.6.11', :platform => :mri_18
  gem 'activesupport', '~> 3.x', :platform => :mri_18
  gem 'ruby-maven', '~> 3.1.1.0', :platform => :jruby
  gem 'jar-dependencies', :platform => :jruby
  gem 'rake-compiler'
  gem 'pry'

  # Used for testing `zk` gem based functionality
  gem 'zk', '~> 1.9.4'
end

group :test do
  gem 'rspec', '~> 3.0.0'
  gem 'rspec-its'
  gem 'system_timer', :platform => :mri_18
  # Used for testing encoding protobufs in an out of Hermann in integration
  # tests
  gem 'protobuffy'

  gem 'ci_reporter_rspec'
  gem 'simplecov'
  gem 'simplecov-rcov'
end

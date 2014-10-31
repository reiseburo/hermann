source "https://rubygems.org"

gemspec

group :development do
  gem 'rake'
  gem 'rake-compiler'
  gem 'pry'
end

group :test do
  gem 'rspec', '~> 3.0.0'
  gem 'rspec-its'
  gem 'system_timer', :platform => :mri_18
  # Used for testing encoding protobufs in an out of Hermann in integration
  # tests
  gem 'protobuffy'
end

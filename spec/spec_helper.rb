require 'rubygems'
require 'yaml'
require 'rspec'

# Add ext/ to the load path so we can load `hermann_lib`
$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + '/../ext/'))
$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + '/../lib/'))

require 'hermann'

RSpec.configure do |c|
  c.color = true
  c.formatter = :documentation

  shared_context 'integration test context', :type => :integration do
    let(:topic) { $integrationconf['kafka']['topic'] }
    let(:brokers) { $integrationconf['kafka']['brokers'] }
  end
end

integration_config = File.expand_path(File.dirname(__FILE__) + '/fixtures/integration.yml')
if File.exists?(integration_config)
  $integrationconf = YAML.load_file(integration_config)
end

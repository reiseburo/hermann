require 'rubygems'

# Add ext/ to the load path so we can load `hermann_lib`
$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + '/../ext/'))

require 'hermann'

RSpec.configure do |c|
end

require 'rubygems'

# Add ext/ to the load path so we can load `hermann_lib`
$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + '/../ext/'))
$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + '/../lib/'))

require 'hermann'

RSpec.configure do |c|
  c.color = true
  c.formatter = :documentation
end

require 'bundler/setup'
require 'hermann'
require 'hermann/discovery/metadata'

c = Hermann::Discovery::Metadata.new( "localhost:9092" )
puts c.get_topics("maxwell").inspect


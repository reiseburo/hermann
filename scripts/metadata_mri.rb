require 'bundler/setup'
require 'hermann'
require 'hermann/discovery/metadata'

c = Hermann::Discovery::Metadata.new( "localhost:9092" )
c.topic("maxwell")
puts c.topic("maxwell").inspect


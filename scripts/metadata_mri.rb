require 'bundler/setup'
require 'hermann'
require 'hermann/discovery/metadata'
require 'hermann/consumer'

c = Hermann::Discovery::Metadata.new( "localhost:9092" )
c.topic("maxwell")
puts c.topic("maxwell").inspect


puts c.brokers.inspect
consumer = Hermann::Consumer.new("maxwell", brokers: "localhost:9092, localhost:9092", partition: c.topic('maxwell').partitions.first.id, offset: :start)
consumer.consume do |c|
  puts c
end

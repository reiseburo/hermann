# Hermann
## A Ruby gem implementing a Kafka Publisher and Consumer

This library wraps the librdkafka library (https://github.com/edenhill/librdkafka) which is implemented in C.  This library must be installed before we can use the Hermann gem.

### Usage

Usage is modelled on the kafka-rb gem (https://github.com/acrosa/kafka-rb) and is fairly straightforward.

- Kafka 0.7 and Kafka 0.8 are supported.
- Ruby 1.8.7 and Ruby 2.1.1 have been tested, but JRuby and versions >= 1.8 should work as long as the librdkafka library is installed.
- This is an early alpha version of the gem, so expect bumps in the road.

**Note:**  The current implementation needs work to ensure thread safety, as well as changes to the initialization of the Producer so that state is properly maintained.
The current version sets up and tears down the Producer's connection to Kafka with each message.

#### Consumer

*intended API*

require 'hermann'

c = Hermann::Consumer.new( :topic => "device_actions", :brokerlist => "localhost:9092" )

c.consume do {
    |msg| puts("Received: #{msg}")
}

*current API*

require 'hermann'

c = Hermann::Consumer.new( "device_actions" )
c.consume() do
  |msg| puts("Received: #{msg}")
end

#### Producer

require 'hermann'

p = Hermann::Producer.new( "device_actions" )

messages = [ "Locate", "Scream", "Wipe", "Degauss" ]
messages.each {
    |msg| p.push(msg)
}

## Questions?

Stan Campbell can be reached at stan.campbell3 at( @ ) gmail.com



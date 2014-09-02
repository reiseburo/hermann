# Hermann

[![Gitter chat](https://badges.gitter.im/lookout/Hermann.png)](https://gitter.im/lookout/Hermann)

A Ruby gem implementing a Kafka Publisher and Consumer

This library wraps the
[librdkafka library](https://github.com/edenhill/librdkafka) which is
implemented in C.  This library must be installed before we can use
the Hermann gem.

### Usage

Usage is modelled on the
[kafka-rb gem](https://github.com/acrosa/kafka-rb) and is fairly
straightforward.

- Kafka 0.8 is supported.
- Ruby 1.8.7 and Ruby 2.1.1 have been tested, but JRuby and versions
  >= 1.8 should work as long as the librdkafka library is installed.
- This is an early alpha version of the gem, so expect bumps in the
  road.

**Note:**  The current implementation needs work to ensure thread safety.

Both Consumers and Producers now hold their own references to their
Kafka state, and should be able to coexist nicely.

The brokers list and partition specification for Consumers are now
working, as well.  Producers will detect the number of partitions in a
topic and randomly spread their messages over the set of partitions.

Consumers currently do not remember their "last message", nor do they
yet coordinate partition (re)assigment within consumer groups.

#### Consumer

    require 'hermann'

    # Initialize requires topic, brokers list, and partition number

    c = Hermann::Consumer.new( "device_actions", "localhost:9092", 0 )
    c.consume() do
      |msg| puts("Received: #{msg}")
    end

#### Producer

    require 'hermann'

    p = Hermann::Producer.new( "device_actions", "localhost:9092" )

    messages = [ "Locate", "Scream", "Wipe", "Degauss" ]
    messages.each do
        |msg| p.push(msg)
    end

## Questions?

Stan Campbell can be reached at stan.campbell3 at( @ ) gmail.com

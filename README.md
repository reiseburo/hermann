# Hermann

A Ruby gem implementing a Kafka Publisher and Consumer

This library wraps the [librdkafka library](https://github.com/edenhill/librdkafka) which is implemented in C.
This library must be installed before we can use the Hermann gem.

### Usage

Usage is modelled on the kafka-rb gem
(https://github.com/acrosa/kafka-rb) and is fairly straightforward.

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

## License

The MIT License (MIT)

Copyright (c) 2014 Lookout, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

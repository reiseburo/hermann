# Hermann

[![Gitter chat](https://badges.gitter.im/reiseburo/hermann.png)](https://gitter.im/reiseburo/hermann) [![Build Status](https://travis-ci.org/reiseburo/hermann.svg?branch=master)](https://travis-ci.org/reiseburo/hermann)

A Ruby gem implementing a Kafka Publisher and Consumer

On MRI (C-based Ruby), this library wraps the [librdkafka
library](https://github.com/edenhill/librdkafka) which is implemented in C.

On JRuby this library [declares jar
dependencies](https://github.com/mkristian/jar-dependencies/wiki/declare-jars-inside-gemspec)
inside the `.gemspec` to express dependencies on the Java-based Kafka library
provided by the Kafka project. Tools like
[jbundler](https://github.com/mkristian/jbundler) will handle these
declarations correctly.

### Usage

Usage is modelled on the
[kafka-rb gem](https://github.com/acrosa/kafka-rb) and is fairly
straightforward.

- Kafka 0.8 is supported.
- Ruby 1.9.3, 2.1.1 and JRuby are tested against


### Producer

#### Zookeeper discovery

Discover Kafka brokers through zookeeper.  Looks at ```/brokers/ids``` in Zookeeper to find the list of brokers.

```ruby
require 'hermann/producer'
require 'hermann/discovery/zookeeper'

broker_ids_array = Hermann::Discovery::Zookeeper.new('localhost:2181').get_brokers
producer = Hermann::Producer.new('topic', broker_ids_array)

promise = producer.push('hello world') # send message to kafka
promise.value                          # forces the Concurrent::Promise to finish excuting (#value!)
promise.state                          # the state of the promise
```


#### MRI only

```ruby
require 'hermann/producer'

broker_ids_array = Hermann::Discovery::Zookeeper.new('localhost:2181').get_brokers
p = Hermann::Producer.new('topic', broker_ids_array)  # arguments topic, list of brokers
f = p.push('hello world from mri')
f.state
p.tick_reactor
f.state
```

### Consumer

Messages can be consumed by calling the consume method and passing a block to handle the yielded messages.  The consume method blocks, so take care to handle that functionality appropriately (i.e. use Concurrent::Promise, Thread, etc).

#### (JRuby)
```ruby
require 'hermann'
require 'hermann/consumer'
require 'hermann_jars'

topic     = 'topic'
new_topic = 'other_topic'

the_consumer = Hermann::Consumer.new(topic, zookeepers: "localhost:2181", group_id: "group1")

the_consumer.consume(new_topic) do |msg|   # can change topic with optional argument to .consume
  puts "Recv: #{msg}"
end
```


#### (MRI)

MRI currently has no zookeeper / client group support.

```ruby
require 'hermann'
require 'hermann/consumer'

topic     = 'topic'
new_topic = 'other_topic'

the_consumer = Hermann::Consumer.new(topic, brokers: "localhost:9092", partition: 1)

the_consumer.consume(new_topic) do |msg, key, offset|   # can change topic with optional argument to .consume
  puts "Recv: #{msg}, key: #{key}, offset: #{offset}"
end
```

### Metadata request (MRI-only)

Topic and cluster metadata may be retrieved in the MRI version by querying the Kafka brokers.

```ruby
require 'hermann'
require 'hermann/discovery/metadata'

c = Hermann::Discovery::Metadata.new( "localhost:9092" )
topic = c.topic("topic")

puts topic.partitions.first

consumers = topic.partitions.map do |partition|
  partition.consumer
end

```

#### Build & Unit Test 

First time (from a clean repository): 
`bundle install && bundle exec rake`

Thereafter: 
`bundle exec rake spec`

#### Testing

To run the integration tests:
 * startup your own instance of zookeeper/kafka
 * `rspec spec/integration/producer_spec.rb`


#### How to convert from using jruby-kafka

* Gemfile
  * remove jruby-kafka
  * add ```gem "hermann"```
  * ```bundle install```
* Jarfile
  * removed unecessary jars from your Jarfile (i.e. kafka, log4j)
  * jar dependencies are automatically included with Hermann
  * ```jbundle install```
* Test out one of the Producer/Consumer examples above






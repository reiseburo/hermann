# Hermann

[![Gitter chat](https://badges.gitter.im/lookout/Hermann.png)](https://gitter.im/lookout/Hermann)

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
- This is an early alpha version of the gem, so expect bumps in the
  road.


### Producer

#### Zookeeper discovery (JRuby-only)

```
[1] pry(main)> 
[2] pry(main)> require 'hermann/producer'
=> true
[3] pry(main)> require 'hermann/discovery/zookeeper'
=> true
[4] pry(main)> p = Hermann::Producer.new('topic', Hermann::Discovery::Zookeeper.new('localhost:2181').get_brokers)
=> #<Hermann::Producer:0x09e2ad91
 @brokers="localhost:6667",
 @children=[],
 @internal=#<Hermann::Provider::JavaProducer:0x064524dd @producer=#<Java::KafkaJavaapiProducer::Producer:0x79d06bbd>, @topic="topic">,
 @topic="topic">
[5] pry(main)> f = p.push('hello world')
=> #<Concurrent::Promise:0x6c42f2a1
 @children=[],
 @event=#<Concurrent::Event:0x17a703f5 @condition=#<Concurrent::Condition:0x5ff2b8ca @condition=#<ConditionVariable:0x618ad2aa>>, @mutex=#<Mutex:0x1aa6e3c0>, @set=false>,
 @executor=#<Concurrent::ThreadPoolExecutor:0x3531f3ca @executor=#<Java::JavaUtilConcurrent::ThreadPoolExecutor:0x7fcf294e>, @max_queue=30, @overflow_policy=:abort>,
 @mutex=#<Mutex:0x59e43e8c>,
 @on_fulfill=#<Proc:0x2caa5d7c@/home/tyler/.rvm/gems/jruby-1.7.15@rubygems/gems/concurrent-ruby-0.7.0-java/lib/concurrent/promise.rb:38>,
 @on_reject=#<Proc:0x5e671e20@/home/tyler/.rvm/gems/jruby-1.7.15@rubygems/gems/concurrent-ruby-0.7.0-java/lib/concurrent/promise.rb:39>,
 @parent=nil,
 @promise_body=#<Proc:0x38fc5554@/usr/home/tyler/source/github/ruby/Hermann/lib/hermann/provider/java_producer.rb:36>,
 @state=:pending>
[6] pry(main)> f.value
=> nil
[7] pry(main)> f.state
=> :fulfilled
[8] pry(main)> 
```


#### MRI only

```
[1] pry(main)> require 'hermann/producer'
=> true
[2] pry(main)> p = Hermann::Producer.new('topic', 'localhost:6667')
=> #<Hermann::Producer:0x00000805e05aa8 @brokers="localhost:6667", @children=[], @internal=#<Hermann::Lib::Producer:0x00000805e05a58>, @topic="topic">
[3] pry(main)> f = p.push('hello world from mri')
=> #<Hermann::Result:0x00000805ef9b08 @producer=#<Hermann::Producer:0x00000805e05aa8 @brokers="localhost:6667", @children=[#<Hermann::Result:0x00000805ef9b08 ...>], @internal=#<Hermann::Lib::Producer:0x00000805e05a58>, @topic="topic">, @reason=nil, @state=:unfulfilled, @value=nil>
[4] pry(main)> f.state
=> :unfulfilled
[5] pry(main)> p.tick_reactor
=> 1
[6] pry(main)> f.state
=> :fulfilled
[7] pry(main)> 

```

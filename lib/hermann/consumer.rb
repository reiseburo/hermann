require 'hermann'

if Hermann.jruby?
  require 'hermann/provider/java_simple_consumer'
else
  require 'hermann_lib'
end

module Hermann
  class Consumer
    attr_reader :topic, :brokers, :partition, :internal


    # Instantiate Consumer
    #
    # @params [String] kafka topic
    #
    # @params [String] group ID
    #
    # @params [String] comma separated zookeeper list
    #
    # @params [Hash] options for consumer
    # @option opts [String] :brokers   (for MRI) Comma separated list of brokers
    # @option opts [String] :partition (for MRI) The kafka partition
    # @option opts [Fixnum]  :sleep_time (Jruby) Time to sleep between consume retries, defaults to 1sec
    # @option opts [Boolean] :do_retry (Jruby) Retry consume attempts if exceptions are thrown, defaults to true
    def initialize(topic, groupId, zookeepers, opts={})
      @topic = topic
      @brokers = brokers
      @partition = partition

      if Hermann.jruby?
        @internal = Hermann::Provider::JavaSimpleConsumer.new(zookeepers, groupId, topic, opts)
      else
        brokers   = opts.delete(:brokers)
        partition = opts.delete(:partition)
        @internal = Hermann::Lib::Consumer.new(topic, brokers, partition)
      end
    end

    def consume(topic=nil, &block)
      @internal.consume(topic, &block)
    end
  end
end

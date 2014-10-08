require 'hermann'

unless Hermann.jruby?
  require 'hermann_lib'
end

module Hermann
  class Consumer
    attr_reader :topic, :brokers, :partition, :internal

    def initialize(topic, brokers, partition)
      @topic = topic
      @brokers = brokers
      @partition = partition
      unless Hermann.jruby?
        @internal = Hermann::Lib::Consumer.new(topic, brokers, partition)
      end
    end

    def consume(&block)
      @internal.consume(&block)
    end
  end
end

require 'hermann'
require 'hermann_lib'

module Hermann
  class Consumer
    attr_reader :topic, :brokers, :partition

    def initialize(topic, brokers, partition)
      @topic = topic
      @brokers = brokers
      @partition = partition
    end

    def consume
    end
  end
end

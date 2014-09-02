require 'hermann'
require 'hermann_lib'

module Hermann
  class Producer
    attr_reader :topic, :brokers, :internal

    def initialize(topic, brokers)
      @topic = topic
      @brokers = brokers
      @internal = Hermann::Lib::Producer.new(topic, brokers)
    end

    # Push a value onto the Kafka topic passed to this +Producer+
    #
    # @param [Array] value An array of values to push, will push each one
    #   separately
    # @param [Object] value A single object to push
    # @return [Object] the value passed in
    def push(value)
      if value.kind_of? Array
        value.each { |element| self.push(element) }
      else
        @internal.push_single(value)
      end

      return value
    end
  end
end

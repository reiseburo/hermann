require 'hermann'
require 'hermann/result'
require 'hermann_lib'

module Hermann
  class Producer
    attr_reader :topic, :brokers, :internal, :children

    def initialize(topic, brokers)
      @topic = topic
      @brokers = brokers
      @internal = Hermann::Lib::Producer.new(topic, brokers)
      # We're tracking children so we can make sure that at Producer exit we
      # make a reasonable attempt to clean up outstanding result objects
      @children = []
    end

    # Push a value onto the Kafka topic passed to this +Producer+
    #
    # @param [Array] value An array of values to push, will push each one
    #   separately
    # @param [Object] value A single object to push
    # @return [Hermann::Result] A future-like object which will store the
    #   result from the broker
    def push(value)
      result = create_result

      if value.kind_of? Array
        return value.map do |element|
          self.push(element)
        end
      else
        @internal.push_single(value, result)
      end

      return result
    end

    # Create a +Hermann::Result+ that is tracked in the Producer's children
    # array
    #
    # @return [Hermann::Result] A new, unused, result
    def create_result
      @children << Hermann::Result.new(self)
      return @children.last
    end


    # Tick the underlying librdkafka reacter and clean up any unreaped but
    # reapable children results
    #
    # @param [FixNum] timeout Milliseconds to block on the internal reactor
    # @return [FixNum] Number of +Hermann::Result+ children reaped
    def tick_reactor(timeout=0)
      # Filter all children who are no longer pending/fulfilled
      total_children = @children.size
      @children = @children.reject { |c| c.reap? }
      reaped = total_children - children.size

      # Punt rd_kafka reactor
      @internal.tick(timeout)
      return reaped
    end


    # Creates a new Ruby thread to tick the reactor automatically
    #
    # @param [Hermann::Producer] producer
    # @return [Thread] thread created for ticking the reactor
    def self.run_reactor_for(producer)
    end
  end
end

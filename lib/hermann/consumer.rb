require 'hermann'
require 'hermann/errors'

if Hermann.jruby?
  require 'hermann/provider/java_simple_consumer'
else
  require 'hermann_rdkafka'
end

module Hermann
  # Hermann::Consumer provides a simple consumer API which is only safe to be
  # executed in a single thread
  class Consumer
    attr_reader :topic, :internal


    # Instantiate Consumer
    #
    # @params [String] kafka topic
    # @params [Hash] options for Consumer
    # @option opts [String]        :brokers    (for MRI) Comma separated list of brokers
    # @option opts [Integer]       :partition  (for MRI) The kafka partition
    # @option opts [Symbol|Fixnum] :offset     (for MRI) Starting consumer offset.  either :start, :end, or Fixnum
    # @option opts [Integer] :zookeepers (for jruby) list of zookeeper servers
    # @option opts [Integer] :group_id   (for jruby) client group_id
    #
    def initialize(topic, opts = {})
      @topic = topic

      offset = opts.delete(:offset)
      raise Hermann::Errors::InvalidOffsetError.new("Bad offset: #{offset}") unless valid_offset?(offset)

      if Hermann.jruby?
        zookeepers, group_id = require_values_at(opts, :zookeepers, :group_id)

        @internal = Hermann::Provider::JavaSimpleConsumer.new(zookeepers, group_id, topic, opts)
      else
        brokers, partition = require_values_at(opts, :brokers, :partition)

        @internal = Hermann::Provider::RDKafka::Consumer.new(topic, brokers, partition, offset)
      end
    end

    # Delegates the consume method to internal consumer classes
    def consume(topic=nil, &block)
      @internal.consume(topic, &block)
    end

    # Delegates the shutdown of kafka messages threads to internal consumer classes
    def shutdown
      if Hermann.jruby?
        @internal.shutdown
      else
        #no op
      end
    end

    private

    def valid_offset?(offset)
      offset.nil? || offset.is_a?(Fixnum) || offset == :start || offset == :end
    end

    def require_values_at(opts, *args)
      args.map do |a|
        raise "Please provide :#{a} option!" unless opts[a]
        opts.delete(a)
      end
    end
  end
end

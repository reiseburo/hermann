require 'hermann'
require 'concurrent'
require 'json'

module Hermann
  module Provider

    # This class simulates the kafka producer class within a java environment.
    # If the producer throw an exception within the Promise a call to +.value!+
    # will raise the exception and the rejected flag will be set to true
    #
    class JavaProducer
      attr_accessor :producer

      def initialize(brokers)
        properties  = create_properties(:brokers => brokers)
        config      = create_config(properties)
        @producer   = JavaApiUtil::Producer.new(config)
      end

      DEFAULTS = {
                    :string_encoder => 'kafka.serializer.StringEncoder',
                    :partitioner    => 'kafka.producer.DefaultPartitioner',
                    :required_acks  => "1"
                  }.freeze

      # Push a value onto the Kafka topic passed to this +Producer+
      #
      # @param [Object] value A single object to push
      # @param [String] topic to push message to
      #
      # @return +Concurrent::Promise+ Representa a promise to send the
      #   data to the kafka broker.  Upon execution the Promise's status
      #   will be set
      def push_single(msg, topic, unused)
        Concurrent::Promise.execute {
          data = ProducerUtil::KeyedMessage.new(topic, msg)
          @producer.send(data)
        }
      end

      # No-op for now
      def connected?
        return false
      end

      # No-op for now
      def errored?
        return false
      end

      # No-op for now
      def connect(timeout=0)
        nil
      end

      private
        # @return [ProducerConfig] - packaged config for +Producer+
        def create_config(properties)
          ProducerUtil::ProducerConfig.new(properties)
        end

        # @return [Properties] properties object for creating +ProducerConfig+
        def create_properties(args={})
          brokers     = args[:brokers]
          str_encoder = DEFAULTS[:string_encoder]
          partitioner = DEFAULTS[:partitioner]
          acks        = DEFAULTS[:required_acks]

          properties = JavaUtil::Properties.new
          properties.put('metadata.broker.list',  brokers)
          properties.put('serializer.class',      str_encoder)
          properties.put('partitioner.class',     partitioner)
          properties.put('request.required.acks', acks)
          properties
        end
    end
  end
end

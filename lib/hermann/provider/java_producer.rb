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


      # Instantiate JavaProducer
      #
      # @params [String] list of brokers
      #
      # @params [Hash] hash of kafka attributes, overrides defaults
      #
      # @raises [RuntimeError] if brokers string is nil/empty
      #
      # ==== Examples
      #
      # JavaProducer.new('0:9092', {'request.required.acks' => '1'})
      #
      def initialize(brokers, opts={})
        properties  = create_properties(brokers, opts)
        config      = create_config(properties)
        @producer   = JavaApiUtil::Producer.new(config)
      end

      DEFAULTS = {
                    'serializer.class'      => 'kafka.serializer.StringEncoder',
                    'partitioner.class'     => 'kafka.producer.DefaultPartitioner',
                    'request.required.acks' => '1'
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

        # Creates a ProducerConfig object
        #
        # @param [Properties] object with broker properties
        #
        # @return [ProducerConfig] - packaged config for +Producer+
        def create_config(properties)
          ProducerUtil::ProducerConfig.new(properties)
        end

        # Creates Properties Object
        #
        # @param [Hash] brokers passed into this function
        # @option args [String] :brokers - string of brokers
        #
        # @return [Properties] properties object for creating +ProducerConfig+
        #
        # @raises [RuntimeError] if options does not contain key value strings
        def create_properties(brokers, opts={})
          brokers = { 'metadata.broker.list' => brokers }
          options = DEFAULTS.merge(brokers).merge(opts)
          properties = JavaUtil::Properties.new
          options.each do |key, val|
            validate_property!(key, val)
            properties.put(key, val)
          end
          properties
        end

        def validate_property!(key, val)
          if key.to_s.empty? || val.to_s.empty?
            raise Hermann::Errors::ConfigurationError, "Invalid Broker Properties"
          end
        end
    end
  end
end

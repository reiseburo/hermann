require 'json'
require 'hermann'
require 'hermann/errors'

module Hermann
  module Provider
    # This class simulates the kafka producer class within a java environment.
    # If the producer throw an exception within the Promise a call to +.value!+
    # will raise the exception and the rejected flag will be set to true
    #
    class JavaProducer
      attr_accessor :producer

      #default kafka Producer options
      DEFAULTS = {
                    'partitioner.class'     => 'kafka.producer.DefaultPartitioner',
                    'request.required.acks' => '1',
                    'message.send.max.retries' => '0'
                  }.freeze

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
        config      = create_config(brokers, opts)
        @producer   = JavaApiUtil::Producer.new(config)
      end

      # Push a value onto the Kafka topic passed to this +Producer+
      #
      # @param [Object] value A single object to push
      # @param [String] topic to push message to
      #
      # @return +Concurrent::Promise+ Representa a promise to send the
      #   data to the kafka broker.  Upon execution the Promise's status
      #   will be set
      def push_single(msg, topic, key, _)
        key = key && key.to_java
        Concurrent::Promise.execute {
          data = ProducerUtil::KeyedMessage.new(topic, nil, key, msg.to_java_bytes)
          begin
            @producer.send(data)
          rescue Java::KafkaCommon::FailedToSendMessageException => jexc
            raise Hermann::Errors::ConnectivityError.new(jexc.message,
                                                         :java_exception => jexc)
          rescue => e
            raise Hermann::Errors::GeneralError.new(e.message,
                                                         :java_exception => e)
          end
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
        # @param [String] comma separated list of brokers
        #
        # @param [Hash] brokers passed into this function
        # @option args [String] :brokers - string of brokers
        #
        # @return [ProducerConfig] - packaged config for +Producer+
        #
        # @raises [RuntimeError] if options does not contain key value strings
        def create_config(brokers, opts={})
          brokers    = { 'metadata.broker.list' => brokers }
          options    = DEFAULTS.merge(brokers).merge(opts)
          properties = Hermann.package_properties(options)
          ProducerUtil::ProducerConfig.new(properties)
        end
    end
  end
end

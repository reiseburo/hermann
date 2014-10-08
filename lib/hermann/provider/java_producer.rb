require 'hermann'
require 'concurrent'
require 'zk'
require 'json'

module Hermann
  module Provider

    # This class simulates the kafka producer class within a java environment.
    # If the producer throw an exception within the Promise a call to +.value!+
    # will raise the exception and the rejected flag will be set to true
    #
    class JavaProducer
      attr_accessor :topic, :producer

      BROKERS_PATH = "/brokers/ids"

      def initialize(topic, zookeepers)
        @topic      = topic
        brokers     = broker_list(zookeepers)
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
      #
      # @return +Concurrent::Promise+ Representa a promise to send the
      #   data to the kafka broker.  Upon execution the Promise's status
      #   will be set
      def push_single(msg)
        Concurrent::Promise.execute {
          data = ProducerUtil::KeyedMessage.new(@topic, msg)
          @producer.send(data)
        }
      end

      # Get a list of brokers
      #
      # @return [String] a csv of Broker's host:port
      def broker_list(zookeepers)
        get_all_zookeepers_brokers(zookeepers).map {|b| "#{b["host"]}:#{b["port"]}" }.join(",")
      end

      # Create connection to Zookeepers
      #
      # @return [Array] parsed json containing host and port of brokers
      def get_all_zookeepers_brokers(zookeepers)
        ZK.open(zookeepers) { |zk| return get_zookeeper_brokers(zk) }
      end

      # Get broker's data
      #
      # @return [Array] parsed json container host and port of brokers
      def get_zookeeper_brokers(zookeeper)
        zookeeper.children(BROKERS_PATH).inject([]) do |brokers, id|
          broker_json = zookeeper.get("#{BROKERS_PATH}/#{id}")[0]
          brokers << JSON.parse(broker_json)
        end
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

require 'java'
require 'hermann'
require 'concurrent'

module Hermann
  module Provider
    class JavaProducer
      attr_accessor :topic, :producer

      def initialize(topic, brokers)
        @topic     = topic
        properties = create_properties(:brokers => brokers)
        config     = create_config(properties)
        @producer  = JavaApiUtil::Producer.new(config)
      end

      DEFAULTS = {
                    :string_encoder => 'kafka.serializer.StringEncoder',
                    :partitioner    => 'kafka.producer.DefaultPartitioner',
                    :required_acks  => "1"
                  }.freeze

      def push_single(msg)
        Concurrent::Promise.new {
          data = ProducerUtil::KeyedMessage.new(@topic, msg)
          @producer.send(data)
        }.rescue { |reason| raise reason }
      end

      private
        def create_config(properties)
          ProducerUtil::ProducerConfig.new(properties)
        end

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
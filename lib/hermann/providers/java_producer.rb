require 'java'
require 'hermann'
require 'concurrent'

module JavaUtil
  include_package 'java.util'
end
module ProducerUtil
  include_package 'kafka.producer'
end
module JavaApiUtil
  include_package 'kafka.javaapi.producer'
end


module Hermann
  module Providers
    class JavaProducer
      attr_accessor :topic, :producer

      def initialize(topic, brokers)
        @topic     = topic
        properties = create_properties(brokers: brokers)
        config     = create_config(properties)
        @producer  = JavaApiUtil::Producer.new(config)
      end

      def defaults
        {
          string_encoder: 'kafka.serializer.StringEncoder',
          partitioner: 'kafka.producer.DefaultPartitioner',
          required_acks: "1"
        }
      end

      def create_config(properties)
        ProducerUtil::ProducerConfig.new(properties)
      end

      def create_properties(args={})
        brokers     = args[:brokers]
        str_encoder = defaults[:string_encoder]
        partitioner = defaults[:partitioner]
        acks        = defaults[:required_acks]

        properties = JavaUtil::Properties.new
        properties.put('metadata.broker.list',  brokers)
        properties.put('serializer.class',      str_encoder)
        properties.put('partitioner.class',     partitioner)
        properties.put('request.required.acks', acks)
        properties
      end

      def push(*messages)
        messages.flatten.map do |msg|
          _push(msg)
        end
      end
      def push_single(msg, result)
        _push(msg)
      end

      private
        def _push(msg)
          data = ProducerUtil::KeyedMessage.new(@topic, msg)
          @producer.send(data)
        end
    end
  end
end
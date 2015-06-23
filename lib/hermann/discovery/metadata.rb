require 'hermann_rdkafka'
require 'hermann/consumer'

module Hermann
  module Discovery
    class Metadata
      Broker    = Struct.new(:id, :host, :port) do
        def to_s
          "#{host}:#{port}"
        end
      end
      Topic     = Struct.new(:name, :partitions)

      Partition = Struct.new(:id, :leader, :replicas, :insync_replicas, :topic_name) do
        def consumer(offset=:end)
          Hermann::Consumer.new(topic_name, brokers: ([leader] + replicas).join(','), partition: id, offset: offset)
        end
      end

      DEFAULT_TIMEOUT_MS = 2_000
      def initialize(brokers, options = {})
        raise "this is an MRI api only!" if Hermann.jruby?
        @internal = Hermann::Provider::RDKafka::Producer.new(brokers)
        @timeout = options[:timeout] || DEFAULT_TIMEOUT_MS
      end

      #
      # @internal.metadata returns:
      # {:brokers => [{:id=>3, :host=>"kafka3.alpha4.sac1.zdsys.com", :port=>9092}],
      #  :topics  => {"testtopic"=>[{:id=>0, :leader_id=>3, :replica_ids=>[3, 1],  :isr_ids=>[3, 1]}}}
      #
      def brokers
        brokers_from_metadata(@internal.metadata(nil, @timeout))
      end

      def topic(t)
        get_topics(t)[t]
      end

      def topics
        get_topics
      end

      private

      def get_topics(filter_topics = nil)
        md = @internal.metadata(filter_topics, @timeout)

        broker_hash = brokers_from_metadata(md).inject({}) do |h, broker|
          h[broker.id] = broker
          h
        end

        md[:topics].inject({}) do |topic_hash, arr|
          topic_name, raw_partitions = *arr
          partitions = raw_partitions.map do |p|
            leader       = broker_hash[p[:leader_id]]
            all_replicas = p[:replica_ids].map { |i| broker_hash[i] }
            isr_replicas = p[:isr_ids].map { |i| broker_hash[i] }
            Partition.new(p[:id], leader, all_replicas, isr_replicas, topic_name)
          end

          topic_hash[topic_name] = Topic.new(topic_name, partitions)
          topic_hash
        end
      end


      def brokers_from_metadata(md)
        md[:brokers].map do |h|
          Broker.new(h[:id], h[:host], h[:port])
        end
      end

    end
  end
end

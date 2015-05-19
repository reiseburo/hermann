require 'hermann_lib'

module Hermann
  module Discovery
    class Metadata
      Broker    = Struct.new(:id, :host, :port)
      Topic     = Struct.new(:name, :partitions)
      Partition = Struct.new(:id, :leader, :replicas, :insync_replicas)

      TIMEOUT_MS = 200
      def initialize(brokers)
        raise "this is an MRI api only!" if Hermann.jruby?
        @internal = Hermann::Lib::Producer.new(brokers)
      end

      #
      # @internal.metadata returns:
      # {:brokers => [{:id=>3, :host=>"kafka3.alpha4.sac1.zdsys.com", :port=>9092}],
      #  :topics  => {"testtopic"=>[{:id=>0, :leader_id=>3, :replica_ids=>[3, 1],  :isr_ids=>[3, 1]}}}
      #
      def brokers
        brokers_from_metadata(@internal.metadata("", TIMEOUT_MS))
      end

      def topics(filter_topics = nil)
        md = @internal.metadata(filter_topics, TIMEOUT_MS)

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
            Partition.new(p[:id], leader, all_replicas, isr_replicas)
          end

          topic_hash[topic_name] = Topic.new(topic_name, partitions)
          topic_hash
        end
      end

      def topic(t)
        topics(t)[t]
      end

      private

      def brokers_from_metadata(md)
        md[:brokers].map do |h|
          Broker.new(h[:id], h[:host], h[:port])
        end
      end

    end
  end
end

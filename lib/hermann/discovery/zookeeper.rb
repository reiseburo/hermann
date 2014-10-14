require 'hermann'
require 'zk'
require 'json'
require 'hermann/errors'

module Hermann
  module Discovery


    # Communicates with Zookeeper to discover kafka broker ids
    #
    class Zookeeper
      attr_reader :zookeepers

      BROKERS_PATH = "/brokers/ids".freeze

      def initialize(zookeepers)
        @zookeepers = zookeepers
      end

      # Gets comma separated string of brokers
      #
      # @param [Fixnum] timeout to connect to zookeeper, "2 times the
      #    tickTime (as set in the server configuration) and a maximum
      #    of 20 times the tickTime2 times the tick time set on server"
      #
      # @return [String] comma separated list of brokers
      #
      # @raises [NoBrokersError] if could not discover brokers thru zookeeper
      def get_brokers(timeout=0)
        brokers = []
        ZK.open(zookeepers, {:timeout => timeout}) do |zk|
          brokers = fetch_brokers(zk)
        end
        if brokers.empty?
          raise Hermann::Errors::NoBrokersError
        end
        brokers.join(',')
      end

      private

        # Gets an Array of broker strings
        #
        # @param [ZK::Client] zookeeper client
        #
        # @return array of broker strings
        def fetch_brokers(zk)
          brokers = []
          zk.children(BROKERS_PATH).each do |id|
            node = fetch_znode(zk, id)
            next if node.nil? # whatever error could happen from ZK#get
            brokers << format_broker_from_znode(node)
          end
          brokers.compact
        end

        # Gets node from zookeeper
        #
        # @param [ZK::Client] zookeeper client
        # @param [Fixnum] kafka broker
        #
        # @return [String] node data
        def fetch_znode(zk, id)
          zk.get("#{BROKERS_PATH}/#{id}")[0]
        rescue ZK::Exceptions::NoNode
          nil
        end

        # Formats the node data into string
        #
        # @param [String] node data
        #
        # @return [String] formatted node data or empty string if error
        def format_broker_from_znode(znode)
          hash = JSON.parse(znode)
          host = hash['host']
          port = hash['port']
          host && port ? "#{host}:#{port}" : nil
        rescue JSON::ParserError
          nil
        end
    end
  end
end
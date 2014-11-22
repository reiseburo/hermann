require 'hermann'

if RUBY_PLATFORM == 'java'
  require 'java'
end

require 'json'
require 'hermann/errors'

module Hermann
  module Discovery
    # Communicates with Zookeeper to discover kafka broker ids
    #
    class Zookeeper
      attr_reader :zookeepers, :impl

      BROKERS_PATH = "/brokers/ids".freeze
      def initialize(zookeepers)
        @zookeepers = zookeepers
        @impl =  nil
        if CuratorImpl.usable?
          @impl = CuratorImpl.new(zookeepers)
        elsif ZkGemImpl.usable?
          @impl = ZkGemImpl.new(zookeepers)
        else
          raise Hermann::Errors::GeneralError, "Could not find a usable Zookeeper implementation, please make sure either the `zk` gem is installed or Curator is on the classpath"
        end
      end

      # Gets comma separated string of brokers
      #
      # @param [Fixnum] timeout to connect to zookeeper, "2 times the
      #    tickTime (as set in the server configuration) and a maximum
      #    of 20 times the tickTime2 times the tick time set on server"
      #
      # @return [Array] List of brokers from ZK
      # @raises [NoBrokersError] if could not discover brokers thru zookeeper
      def get_brokers(timeout=0)
        brokers = impl.brokers(timeout).map { |b| format_broker_from_znode(b) }

        if brokers.empty?
          raise Hermann::Errors::NoBrokersError
        end
        return brokers
      end

      private

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

      # The ZkGemImpl class is an implementation of simple broker discovery
      # using the `zk` gem if it is available
      class ZkGemImpl
        def self.usable?
          begin
            require 'zk'
            return true
          rescue LoadError
            return false
          end
        end

        def initialize(zks)
          @zookeepers = zks
        end

        def brokers(timeout=0)
          ZK.open(@zookeepers, {:timeout => timeout}) do |zk|
            return fetch_brokers(zk)
          end
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
            brokers << node
          end
          return brokers
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
      end

      # The CuratorImpl is an implementation of simple broker discovery using
      # Apache Curator libraries, if they are made available on the classpath
      # for the process running Hermann::Discovery::Zookeeper.
      #
      # For a number of reasons this is preferred over the `zk` gem, namely
      # being a much more simple and mature Zookeeper client interface
      class CuratorImpl
        def self.usable?
          begin
            Java::OrgApacheCuratorFramework::CuratorFrameworkFactory
            return true
          rescue NameError
            return false
          end
        end

        def initialize(zks)
          retry_policy = Java::OrgApacheCuratorRetry::ExponentialBackoffRetry.new(1000, 3)
          @curator = Java::OrgApacheCuratorFramework::CuratorFrameworkFactory.newClient(zks, retry_policy)
        end

        # Timeout is discarded, only later versions of Curator support
        # blockUntilConnected which would take the timeout variable
        def brokers(timeout=0)
          unless @curator.started?
            @curator.start
          end

          brokers = []
          @curator.children.for_path(BROKERS_PATH).each do |id|
            path = "#{BROKERS_PATH}/#{id}"
            brokers << @curator.data.for_path(path).to_s
          end
          return brokers
        end
      end
    end
  end
end


module Hermann
  module Errors
    # Error for connectivity problems with the Kafka brokers
    class ConnectivityError; end;

    # For passing incorrect config and options to kafka
    class ConfigurationError < StandardError; end

    # cannot discover brokers from zookeeper
    class NoBrokersError < StandardError; end
  end
end


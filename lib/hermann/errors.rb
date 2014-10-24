
module Hermann
  module Errors
    # Error for connectivity problems with the Kafka brokers
    class ConnectivityError < StandardError
      attr_reader :java_exception

      # Initialize a connectivity error
      #
      # @param [String] message Exception's message
      # @param [Hash[ options
      # @option options [Java::Lang::RuntimeException] :java_exception An
      #   underlying Java exception
      def initialize(message, options={})
        super(message)
        @java_exception = options[:java_exception]
      end
    end

    # For passing incorrect config and options to kafka
    class ConfigurationError < StandardError; end

    # cannot discover brokers from zookeeper
    class NoBrokersError < StandardError; end
  end
end


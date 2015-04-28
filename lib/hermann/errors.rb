
module Hermann
  module Errors
    class GeneralError < StandardError
      attr_reader :java_exception

      # Initialize a connectivity error
      #
      # @param [String] message Exception's message
      # @param [Hash[ options
      # @option options [Java::Lang::RuntimeException] :java_exception An
      #   underlying Java exception
      def initialize(message='', options={})
        super(message)
        @java_exception = options[:java_exception]
      end
    end

    # Error for connectivity problems with the Kafka brokers
    class ConnectivityError < GeneralError; end

    # For passing incorrect config and options to kafka
    class ConfigurationError < GeneralError; end

    # cannot discover brokers from zookeeper
    class NoBrokersError < GeneralError; end

    # offsets can only be two symbols or a fixnum
    class InvalidOffsetError < GeneralError; end
  end
end


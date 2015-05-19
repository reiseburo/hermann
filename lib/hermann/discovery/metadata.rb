require 'hermann_lib'

module Hermann
  module Discovery
    class Metadata
      TIMEOUT_MS = 200
      def initialize(brokers)
        raise "this is an MRI api only!" if Hermann.jruby?
        @internal = Hermann::Lib::Producer.new(brokers)
      end

      def get_topics(filter_topics = nil)
        @internal.metadata(filter_topics, TIMEOUT_MS)
      end
    end
  end
end

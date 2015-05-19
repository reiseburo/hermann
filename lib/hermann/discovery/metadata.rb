require 'hermann_lib'

module Hermann
  module Discovery
    class Metadata
      def initialize(brokers)
        raise "this is an MRI api only!" if Hermann.jruby?
        @internal = Hermann::Lib::Producer.new(brokers)
      end

      def get_topics
        @internal.metadata(nil, 200)
      end
    end
  end
end

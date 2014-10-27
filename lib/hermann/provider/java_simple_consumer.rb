require 'hermann'
require 'hermann/errors'

module Hermann
  module Provider

    # Implements a java based consumer.  The #consumer method loops infinitely,
    # the hasNext() method blocks until a message is available.
    class JavaSimpleConsumer
      attr_accessor :consumer, :topic, :zookeeper

      NUM_THREADS = 1

      #default zookeeper connection options
      DEFAULTS = {
                   'zookeeper.session.timeout.ms' => '400',
                   'zookeeper.sync.time.ms'       => '200',
                   'auto.commit.interval.ms'      => '1000'
                  }.freeze

      # Instantiate JavaSimpleConsumer
      #
      # @params [String] list of zookeepers
      #
      # @params [String] Group ID
      #
      # @params [String] Kafka topic
      #
      # @params [Hash] kafka options for consumer
      # @option opts [Fixnum] :sleep_time Time to sleep between consume retries, defaults to 1sec
      # @option opts [Boolean] :do_retry Retry consume attempts if exceptions are thrown, defaults to true
      def initialize(zookeepers, groupId, topic, opts={})
        config       = create_config(zookeepers, groupId)
        @consumer    = ConsumerUtil::Consumer.createJavaConsumerConnector(config)
        @topic       = topic
        @sleep_time  = opts.delete(:sleep_time) || 1
        @do_retry    = opts.delete(:do_retry)   || true
      end

      # Starts infinite loop to consume messages. hasNext() blocks until a
      # message is available at which point it is yielded to the block
      #
      # @params [String] optional topic to override initialized topic
      #
      # ==== Examples
      #
      # consumer.consume do |message|
      #   puts "Received: #{message}"
      # end
      #
      def consume(topic=nil)
        begin
          stream = get_stream(topic)
          it = stream.iterator
          while it.hasNext do
            yield it.next.message.to_s
          end
        rescue Exception => e
          puts "#{self.class.name}#consume exception: #{e.class.name}"
          puts "Msg: #{e.message}"
          puts e.backtrace.join("\n")
          if retry?
            sleep @sleep_time
            retry
          else
            raise e
          end
        end
      end

      private
        def retry?
          @do_retry
        end

        # Gets the message stream of the topic. Creates message streams for
        # a topic and the number of threads requested.  In this case the default
        # number of threads is NUM_THREADS.
        #
        # @params [String] optional topic to override initialized topic
        def get_stream(topic)
          current_topic = topic || @topic
          @topicCountMap = JavaUtil::HashMap.new
          @value         = NUM_THREADS.to_java Java::int
          @topicCountMap.put("#{current_topic}", @value)
          consumerMap = @consumer.createMessageStreams(@topicCountMap)
          consumerMap[current_topic].first
        end

        # Creates a ConsumerConfig object
        #
        # @param [String] zookeepers list
        #
        # @param [String] group ID
        #
        # @return [ConsumerConfig] - packaged config for +Consumer+
        #
        # @raises [RuntimeError] if options does not contain key value strings
        def create_config(zookeepers, groupId, opts={})
          config     = connect_opts(zookeepers, groupId)
          options    = DEFAULTS.merge(config).merge(opts)
          properties = Hermann.package_properties(options)
          ConsumerUtil::ConsumerConfig.new(properties)
        end

        # Connection options to pass to ConsumerConfig
        def connect_opts(zookeepers, groupId)
          {
            'zookeeper.connect' => zookeepers,
            'group.id' => groupId
          }
        end
    end
  end
end

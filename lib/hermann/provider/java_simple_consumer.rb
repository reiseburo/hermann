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
      DEFAULTS_HERMANN_OPTS = {
        'zookeeper.session.timeout.ms' => '400',
        'zookeeper.sync.time.ms'       => '200',
        'auto.commit.interval.ms'      => '1000',
      }.freeze

      DEFAULT_CONSUMER_OPTIONS = {
        :do_retry         => true,
        :max_retries      => 3,
        :backoff_time_sec => 1,
        :logger           => nil
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
      # @option opts [Fixnum]  :backoff_time_sec Time to sleep between consume retries, defaults to 1sec
      # @option opts [Boolean] :do_retry Retry consume attempts if exceptions are thrown, defaults to true
      # @option opts [Fixnum]  :max_retries Number of max_retries to retry #consume when it throws an exception
      # @option opts [Logger]  :logger Pass in a Logger
      # @option opts [Other]   other opts from kafka
      def initialize(zookeepers, groupId, topic, opts={})
        @topic            = topic
        options           = DEFAULT_CONSUMER_OPTIONS.merge(opts)
        @backoff_time_sec = options.delete(:backoff_time_sec)
        @do_retry         = options.delete(:do_retry)
        @max_retries      = options.delete(:max_retries)
        @logger           = options.delete(:logger)
        # deleting options above so that they do not get sent to
        # the create_config method
        config            = create_config(zookeepers, groupId, options)
        @consumer         = ConsumerUtil::Consumer.createJavaConsumerConnector(config)
      end

      # Shuts down the various threads created by createMessageStreams
      # This can be called after the thread executing consume has exited
      # to clean up.
      def shutdown
        @consumer.shutdown
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
            message = it.next.message
            yield String.from_java_bytes(message)
          end
        rescue => e
          if retry? && @max_retries > 0
            sleep @backoff_time_sec
            @max_retries -= 1
            retry
          else
            log_exception(e)
            raise e
          end
        end
      end

      private
        def retry?
          @do_retry
        end

        def log_exception(e)
          return unless @logger
          @logger.error("#{self.class.name}#consume exception: #{e.class.name}")
          @logger.error("Msg: #{e.message}")
          @logger.error(e.backtrace.join("\n"))
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
          options    = DEFAULTS_HERMANN_OPTS.merge(config).merge(opts)
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

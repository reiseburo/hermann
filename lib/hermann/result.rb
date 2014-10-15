
module Hermann
  class Result
    attr_reader :reason, :state

    STATES = [:pending,
              :rejected,
              :fulfilled,
              :unfulfilled,
              ].freeze

    def initialize(producer)
      @producer = producer
      @reason = nil
      @value = nil
      @state = :unfulfilled
    end

    STATES.each do |state|
      define_method("#{state}?".to_sym) do
        return @state == state
      end
    end

    # @return [Boolean] True if this child can be reaped
    def completed?
      return true if rejected? || fulfilled?
      return false
    end

    # Access the value of the future
    #
    # @param [FixNum] timeout Seconds to wait on the underlying machinery for a
    #   result
    # @return [NilClass] nil if no value could be received in the time alotted
    # @return [Object]
    def value(timeout=0)
      @producer.tick_reactor(timeout)
      return @value
    end

    # INTERNAL METHOD ONLY. Do not use
    #
    # This method will be invoked by the underlying extension to indicate set
    # the actual value after a callback has completed
    #
    # @param [Object] value The actual resulting value
    # @param [Boolean] is_error True if the result was errored for whatever
    #   reason
    def internal_set_value(value, is_error)
      @value = value

      if is_error
        puts "Hermann::Result#set_internal_value(#{value.class}:\"#{value}\", error?:#{is_error})"
        @state = :rejected
      else
        @state = :fulfilled
      end
    end

    # INTERNAL METHOD ONLY. Do not use
    #
    # This method will set our internal #reason with the details from the
    # exception
    #
    # @param [Exception] exception
    def internal_set_error(exception)
      return if exception.nil?

      @reason = exception
      @state = :rejected
    end
  end
end

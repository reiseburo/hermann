begin
  require 'system_timer'
  module Hermann
    class Timeout
      USE_SYSTEM_TIMER = true
    end
  end
rescue LoadError
  require 'timeout'

  if RUBY_VERSION == '1.8.7'
    puts ">>> You are running on 1.8.7 without SystemTimer"
    puts ">>> which means Hermann::Timeout will not work as expected"
  end
  module Hermann
    class Timeout
      USE_SYSTEM_TIMER = false
    end
  end
end

module Hermann
  class Timeout
    def self.system_timer?
      Hermann::Timeout::USE_SYSTEM_TIMER
    end

    def self.timeout(seconds, klass=nil, &block)
      if system_timer?
        SystemTimer.timeout_after(seconds, klass, &block)
      else
        ::Timeout.timeout(seconds, klass, &block)
      end
    end
  end
end


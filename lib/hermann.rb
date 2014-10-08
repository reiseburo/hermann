module Hermann
  def self.jruby?
    return RUBY_PLATFORM == "java"
  end

  if self.jruby?
    require 'java'
    require 'hermann_jars'

    module JavaUtil
      include_package 'java.util'
    end
    module ProducerUtil
      include_package 'kafka.producer'
    end
    module JavaApiUtil
      include_package 'kafka.javaapi.producer'
    end
  end
end

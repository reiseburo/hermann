module Hermann
  require 'java'
  require 'hermann_jars'
  require 'concurrent'

  module JavaUtil
    include_package 'java.util'
  end
  module ProducerUtil
    include_package 'kafka.producer'
  end
  module ConsumerUtil
    include_package "kafka.consumer"
  end
  module JavaApiUtil
    include_package 'kafka.javaapi.producer'
  end
  module KafkaUtil
    include_package "kafka.util"
  end
end

require 'spec_helper'

require 'hermann/producer'
require 'hermann/consumer'
require 'hermann/discovery/zookeeper'
require 'concurrent'

class ConsumerTest
  def create_consumer
    zookeeper = "localhost:2181"
    groupId   = "group1"
    topic     = 'test'

    consumer = Hermann::Consumer.new(topic, groupId, zookeeper)

    consumer.consume(topic) do |msg|
      if msg == 'msg'
        consumer.shutdown
        return true
      end
    end
    false
  end
end

describe 'producer' do
  include_context 'integration test context'

  let(:message)   { 'msg' }

  it 'produces and consumes messages', :type => :integration do
    test_consumer = Concurrent::Promise.execute { ConsumerTest.new.create_consumer }
    broker_ids = Hermann::Discovery::Zookeeper.new(zookeepers).get_brokers
    producer = Hermann::Producer.new(nil, broker_ids)
    producer.push(message, :topic => topic).wait(1)
    expect(test_consumer.value(1)).to be true
  end
end

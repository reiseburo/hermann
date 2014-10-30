require 'spec_helper'

require 'hermann/producer'
require 'hermann/consumer'
require 'hermann/discovery/zookeeper'
require 'concurrent'

describe 'producer' do
  include_context 'integration test context'

  let(:timeout) { 10 }
  let(:message)   { 'msg' }
  let(:consumer) do
    Hermann::Consumer.new(topic, 'rspec-group', zookeepers)
  end
  let(:consumer_promise) do
    Concurrent::Promise.execute do
      valid = false
      puts "consuming off `#{topic}`"
      consumer.consume(topic) do |dequeued|
        puts "received the message: #{dequeued}"
        if message == dequeued
          consumer.shutdown
          valid = true
        end
      end
      # Return this out of the block
      next valid
    end
  end

  it 'produces and consumes messages', :type => :integration, :platform => :java do
    broker_ids = Hermann::Discovery::Zookeeper.new(zookeepers).get_brokers
    puts "using ZK discovered brokers: #{broker_ids}"
    producer = Hermann::Producer.new(nil, broker_ids)
    producer.push(message, :topic => topic).value!(timeout)
    expect(consumer_promise.value!(timeout)).to be true
  end

  after :each do
    # Make sure we shut down our connection in any case
    consumer.shutdown
  end
end

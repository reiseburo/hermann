require 'spec_helper'

require 'hermann/producer'
require 'hermann/consumer'
require 'hermann/discovery/zookeeper'

require 'protobuf'

$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + '/../'))
require 'fixtures/testevent.pb'

describe 'producer', :platform => :java  do
  include_context 'integration test context'

  let(:timeout) { 10 }
  let(:message)   { 'msg' }
  let(:consumer) do
    Hermann::Consumer.new(topic, { :group_id => "rspec-group", :zookeepers => zookeepers })
  end
  let(:consumer_promise) do
    Concurrent::Promise.execute do
      value = nil
      puts "consuming off `#{topic}`"
      consumer.consume(topic) do |dequeued|
        puts "received the message: #{dequeued.inspect}"
        value = dequeued
        consumer.shutdown
      end
      # Return this out of the block
      next value
    end
  end
  let(:broker_ids) do
    broker_ids = Hermann::Discovery::Zookeeper.new(zookeepers).get_brokers
    puts ""
    puts "using ZK discovered brokers: #{broker_ids}"
    puts ""
    broker_ids
  end
  let(:producer) { Hermann::Producer.new(nil, broker_ids) }

  it 'produces and consumes messages', :type => :integration, :platform => :java do
    producer.push(message, :topic => topic).value!(timeout)
    expect(consumer_promise.value!(timeout)).to eql(message)
  end


  context 'with binary data', :type => :integration, :platform => :java do
    let(:event) do
      Hermann::TestEvent.new(:name => 'rspec',
                             :state => 3,
                             :bogomips => 9001)
    end

    let(:message) { event.encode }

    it 'should be a thing' do
      producer.push(message, :topic => topic).value!(timeout)
      dequeued = consumer_promise.value!(timeout)
      expect(dequeued).to eql(message)

      expect {
        Hermann::TestEvent.decode(dequeued)
      }.not_to raise_error
    end
  end

  after :each do
    # Make sure we shut down our connection in any case
    consumer.shutdown
  end
end

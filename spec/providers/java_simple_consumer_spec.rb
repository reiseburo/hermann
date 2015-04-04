require 'spec_helper'
require 'hermann/provider/java_simple_consumer'
require 'hermann/errors'

describe Hermann::Provider::JavaSimpleConsumer, :platform => :java  do
  let(:zookeeper)         { 'localhost:2181' }
  let(:groupId)           { 'groupId' }
  let(:topic)             { 'topic' }
  let(:internal_consumer) { double('ConsumerUtil::Consumer') }
  let(:logger)            { double('logger') }
  let(:opts)              { {'foo.bar' => 1 } }

  subject(:consumer) { described_class.new(zookeeper, groupId, topic, {:logger => logger}) }

  before do
    allow(Hermann::ConsumerUtil::Consumer).to receive(:createJavaConsumerConnector).with(any_args) { internal_consumer }
  end

  describe '#consume' do
    let(:stream)   { double }
    let(:iterator) { double }
    let(:msg)      { "rspec-message".to_java_bytes }

    it 'yields messages one at a time' do
      allow(consumer).to receive(:get_stream) { stream }
      allow(stream).to receive(:iterator) { iterator }
      allow(iterator).to receive(:hasNext).and_return(true, false)
      allow(iterator).to receive_message_chain(:next, :message) { msg }

      expect { |b|
        subject.consume(&b)
      }.to yield_with_args(String.from_java_bytes(msg))
    end
    it 'retries consuming if there is an exception' do
      allow(consumer).to receive(:get_stream).and_raise(StandardError)
      expect(consumer).to receive(:sleep).exactly(3).times
      expect(logger).to receive(:error).exactly(3).times
      expect{ |b| consumer.consume(&b) }.to raise_error(StandardError)
    end

    context 'with no logger' do
      subject(:consumer) { described_class.new(zookeeper, groupId, topic, {}) }
      it 'does not call logger' do
        allow(consumer).to receive(:get_stream).and_raise(StandardError)
        allow(consumer).to receive(:sleep)
        expect{ |b| consumer.consume(&b) }.to raise_error(StandardError)
      end
    end
  end

  describe '#get_stream' do
    subject { consumer.send(:get_stream, topic) }

    let(:map) { { topic => ['foo'] } }

    context 'without topic' do
      let(:topic) { nil }
      it 'gets the consumer stream' do
        allow(internal_consumer).to receive(:createMessageStreams) { map }
        expect(subject).to eq 'foo'
      end
    end

    context 'with topic' do
      let(:topic) { 'topic' }
      it 'gets the consumer stream' do
        allow(internal_consumer).to receive(:createMessageStreams) { map }
        expect(map).to receive(:[]).with(topic) { ['foo'] }
        expect(subject).to eq 'foo'
      end
    end
  end

  describe '#create_config' do
    subject { consumer.send(:create_config, zookeeper, groupId, opts) }

    it 'creates the consumer config' do
      expect(subject).to be_a Hermann::ConsumerUtil::ConsumerConfig
      expect(subject.props.to_s).to match "foo.bar"
    end
  end

  describe '#connect_opts' do
    subject { consumer.send(:connect_opts, zookeeper, groupId) }

    it 'creates a hash of connection options' do
      expect(subject).to be_a Hash
    end
  end
end

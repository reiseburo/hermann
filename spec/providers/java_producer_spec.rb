require 'spec_helper'
require 'hermann/provider/java_producer'
require 'hermann/errors'

describe Hermann::Provider::JavaProducer, :platform => :java  do
  subject(:producer) { described_class.new(brokers, opts) }

  let(:topic)      { 'rspec' }
  let(:brokers)    { '0:1337'}
  let(:opts)       { {} }
  let(:part_key)   { "key" }
  let(:msg)        { "bar" }

  describe '#push_single' do
    subject(:result) { producer.push_single(msg, topic, nil, nil) }

    let(:passed_topic) { 'foo' }

    it 'returns an executing Promise' do
      expect(result.wait(1).pending?).to eq false
    end

    it 'can change topic' do
      expect(Hermann::ProducerUtil::KeyedMessage).to receive(:new).with(passed_topic, nil, nil, anything)
      producer.push_single(msg, passed_topic, nil, nil).wait(1)
    end

    it 'can change partition key' do
      expect(Hermann::ProducerUtil::KeyedMessage).to receive(:new).with(passed_topic, nil, part_key, anything)
      producer.push_single(msg, passed_topic, part_key, nil).wait(1)
    end

    context 'error conditions' do
      shared_examples 'an error condition' do
        it 'should be rejected' do
          promise = producer.push_single('rspec', topic, nil, nil).wait(1)
          expect(promise).to be_rejected
          expect { promise.value! }.to raise_error
        end
      end

      context 'with a bad broker configuration' do
        let(:brokers) { '' }
        it 'raises error' do
          expect{described_class.new(brokers)}.to raise_error(Hermann::Errors::ConfigurationError)
        end
      end

      context 'with a non-existing broker' do
        let(:brokers) { 'localhost:13337' }
        it_behaves_like 'an error condition'
      end

      context 'with a bad topic' do
        let(:topic) { '' }
        it_behaves_like 'an error condition'
      end

      context 'when the broker is down' do
        before :each do
          expect(producer.producer).to receive(:send).and_raise(::Java::KafkaCommon::FailedToSendMessageException.new('rspec', nil))
        end

        it 'should raise a ConnectivityError' do
          expect {
            result.value!
          }.to raise_error(Hermann::Errors::ConnectivityError)
        end
      end
    end
  end

  describe '#create_config' do
    let(:opts)    { {'f'=>'1'} }
    let(:options) {
                    Hermann::Provider::JavaProducer::DEFAULTS.merge({
                      "metadata.broker.list"=>brokers, "f"=>"1"
                    })
                  }
    let(:producer_config) { double }

    it 'creates Properties' do
      expect(Hermann).to receive(:package_properties).with(options)
      expect(Hermann::ProducerUtil::ProducerConfig).to receive(:new) { producer_config }
      expect(Hermann::JavaApiUtil::Producer).to receive(:new) { double }
      expect(subject).to_not be_nil
    end
    context 'without brokers' do
      let(:brokers) { '' }
      it 'raises ConfigurationError' do
        expect{ subject }.to raise_error(Hermann::Errors::ConfigurationError)
      end
    end
  end
end

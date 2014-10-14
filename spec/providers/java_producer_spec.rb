require 'spec_helper'
require 'hermann/provider/java_producer'
require 'hermann/errors'

describe Hermann::Provider::JavaProducer, :platform => :java  do
  subject(:producer) { described_class.new(brokers) }

  let(:topic)      { 'rspec' }
  let(:brokers)    { '0:1337'}

  describe '#push_single' do
    subject(:result) { producer.push_single('foo', topic, nil) }

    let(:passed_topic) { 'foo' }
    before do
      allow_any_instance_of(described_class).to receive(:broker_list) { brokers }
    end

    it 'returns an executing Promise' do
      expect(result.wait(1).pending?).to eq false
    end

    it 'can change topic' do
      expect(Hermann::ProducerUtil::KeyedMessage).to receive(:new).with(passed_topic, 'bar')
      producer.push_single('bar', passed_topic, nil).wait(1)
    end

    context 'error conditions' do
      shared_examples 'an error condition' do
        it 'should be rejected' do
          promise = producer.push_single('rspec', topic, nil).wait(1)
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
        let(:timeout) { 2 }
        let(:value) { 'rspec' }

        it 'should reject' do
          future = result.wait(1)
          expect(future).to be_rejected
        end
      end

      context 'with a bad topic' do
        let(:topic) { '' }
        it_behaves_like 'an error condition'
      end
    end
  end

  describe '#create_properties' do
    subject { producer.send(:create_properties, brokers, opts) }
    let(:opts)    { {'f'=>'1'} }
    let(:result)  {
                    Hermann::Provider::JavaProducer::DEFAULTS.merge({
                      "metadata.broker.list"=>brokers, "f"=>"1"
                    })
                  }

    it 'creates Properties' do
      expect(subject).to eq result
    end
    context 'without brokers' do
      let(:brokers) { '' }
      it 'raises ConfigurationError' do
        expect{ subject }.to raise_error(Hermann::Errors::ConfigurationError)
      end
    end
  end

  describe '#validate_property!' do
    subject { producer.send(:validate_property!, foo, bar) }

    context 'with valid property' do
      let(:foo) { 'foo' }
      let(:bar) { 'bar' }
      it 'returns true' do
        expect{ subject }.to_not raise_error
      end
    end

    context 'with valid property' do
      let(:foo) { '' }
      let(:bar) { '' }
      it 'returns false' do
        expect{ subject }.to raise_error(Hermann::Errors::ConfigurationError)
      end
    end
  end
end

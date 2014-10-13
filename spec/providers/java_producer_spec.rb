require 'spec_helper'
require 'hermann/provider/java_producer'

describe Hermann::Provider::JavaProducer, :platform => :java  do
  subject(:producer) { described_class.new(zookeepers) }

  let(:topic)      { 'rspec' }
  let(:zookeepers) { 'localhost:2181' }
  let(:brokers)    { '0:1337'}


  describe '#push_single' do
    subject(:result) { producer.push_single('foo', topic) }

    let(:passed_topic) { 'foo' }
    before do
      allow_any_instance_of(described_class).to receive(:broker_list) { brokers }
    end

    it 'returns an executing Promise' do
      expect(result.wait(1).pending?).to eq false
    end

    it 'can change topic' do
      expect(Hermann::ProducerUtil::KeyedMessage).to receive(:new).with(passed_topic, 'bar')
      producer.push_single('bar', passed_topic).wait(1)
    end

    context 'error conditions' do
      shared_examples 'an error condition' do
        it 'should be rejected' do
          promise = producer.push_single('rspec', topic).wait(1)
          expect(promise).to be_rejected
          expect { promise.value! }.to raise_error
        end
      end

      context 'with a bad broker configuration' do
        let(:brokers) { '' }
        it_behaves_like 'an error condition'
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
end

require 'spec_helper'
require 'hermann/provider/java_producer'
require 'hermann_jars'

describe Hermann::Provider::JavaProducer do
  subject(:producer) { described_class.new(topic, brokers) }

  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }

  describe '#push_single' do
    subject(:result) { producer.push_single(value) }

    context 'error conditions' do
      shared_examples 'an error condition' do
        it 'should be rejected' do
          promise = producer.push_single('rspec').execute.wait(1)
          expect(promise).to be_rejected
          expect(promise.reason).to_not be_nil
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
          future = result.execute.wait(1)
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

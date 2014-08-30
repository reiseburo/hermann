require 'spec_helper'
require 'hermann/producer'

describe Hermann::Producer do
  subject(:producer) { described_class.new(topic, brokers) }

  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }


  describe '#push' do
    context 'error conditions' do
      shared_examples 'an error condition' do
        it 'should raise an exception' do
          expect { producer.push('rspec') }.to raise_error(RuntimeError)
        end
      end

      context 'with a bad broker configuration' do
        let(:brokers) { '' }
        it_behaves_like 'an error condition'
      end

      context 'with a bad topic' do
        let(:topic) { '' }
        it_behaves_like 'an error condition'
      end
    end

    subject(:result) { producer.push(value) }

    context 'with a single value' do
      let(:value) { 'hello' }

      it 'should invoke #push_single' do
        expect(producer.internal).to receive(:push_single)
        expect(result).not_to be_nil
      end
    end

    context 'with an array value' do
      let(:value) { ['hello', 'world'] }

      it 'should invoke #push_single for each element' do
        value.each do |v|
          expect(producer.internal).to receive(:push_single).with(v)
        end

        expect(result).not_to be_nil
      end
    end
  end
end

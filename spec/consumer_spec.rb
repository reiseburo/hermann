require 'spec_helper'
require 'hermann/consumer'

describe Hermann::Consumer do
  subject(:consumer) { described_class.new(topic, brokers, partition) }

  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }
  let(:partition)  { 1 }

  it { should respond_to :consume }

  describe '#consume' do
    shared_examples 'an error condition' do
      it 'should raise an exception' do
        expect { consumer.consume }.to raise_error(RuntimeError)
      end
    end

    context 'with a bad partition' do
      let(:partition) { -1 }
      it_behaves_like 'an error condition'
    end

    context 'with a bad broker' do
      let(:brokers) { '' }
      it_behaves_like 'an error condition'
    end

    context 'with a bad topic' do
      let(:topic) { '' }
      it_behaves_like 'an error condition'
    end
  end
end

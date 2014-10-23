require 'spec_helper'
require 'hermann/consumer'

# XXX: Hermann::Consumer isn't really supported anywhere, MRI included right
# now
describe Hermann::Consumer do
  subject(:consumer) { described_class.new(topic, nil, nil, opts) }

  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }
  let(:partition)  { 1 }
  let(:opts) { { :brokers => brokers, :partition => partition } }


  context "on C ruby", :platform => :mri do
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

  context 'on Jruby', :platform => :java do
    subject(:consumer) { described_class.new(topic, groupId, zookeepers) }

    let(:zookeepers)  { 'localhost:2181' }
    let(:groupId)    { 'groupId' }
    let(:do_retry)   { true }
    let(:sleep_time) { 1 }

    it 'creates a Hermann::Provider::JavaSimpleConsumer' do
      allow(Hermann::ConsumerUtil::Consumer).to receive(:createJavaConsumerConnector).with(any_args) { double }
      expect(subject.internal).to be_a(Hermann::Provider::JavaSimpleConsumer)
    end
  end
end

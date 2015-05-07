require 'spec_helper'
require 'hermann/consumer'
require 'hermann/errors'

# XXX: Hermann::Consumer isn't really supported anywhere, MRI included right
# now
describe Hermann::Consumer do
  subject(:consumer) { described_class.new(topic, opts) }

  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }
  let(:partition)  { 1 }
  let(:offset) { nil }
  let(:opts) { { :brokers => brokers, :partition => partition, :offset => offset } }


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

      context 'with a bad offset' do
        let(:offset) { :foo }
        it "raises an InvalidOffset error" do
          expect { subject }.to raise_error(Hermann::Errors::InvalidOffsetError)
        end
      end
    end

    describe '#shutdown' do
      it 'does nothing' do
        expect(consumer.shutdown).to be nil
      end
    end
  end

  context 'on Jruby', :platform => :java do
    subject(:consumer) { described_class.new(topic, :group_id => groupId, :zookeepers => zookeepers) }

    let(:zookeepers) { 'localhost:2181' }
    let(:groupId)    { 'groupId' }
    let(:do_retry)   { true }
    let(:sleep_time) { 1 }
    let(:internal)   { double('Hermann::Provider::JavaSimpleConsumer')}

    before do
      allow(Hermann::ConsumerUtil::Consumer).to receive(:createJavaConsumerConnector).with(any_args) { double }
    end

    it 'creates a Hermann::Provider::JavaSimpleConsumer' do
      expect(subject.internal).to be_a(Hermann::Provider::JavaSimpleConsumer)
    end

    describe '#shutdown' do
      let(:result) { double('NoClass') }
      it 'calls shutdown' do
        consumer.instance_variable_set(:@internal, internal)
        allow(internal).to receive(:shutdown) { result }
        expect(consumer.shutdown).to eq result
      end
    end
  end
end

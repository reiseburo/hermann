require 'spec_helper'
require 'hermann/producer'

describe Hermann::Producer do
  subject(:producer) { described_class.new(topic, brokers, opts) }

  let(:topic) { 'rspec' }
  let(:brokers) { ['localhost:1337'] }
  let(:opts) { { 'request.required.acks' => '1' } }

  describe '#initialize' do
    context 'with C ruby', :platform => :mri do
      it 'joins broker array' do
        expect(Hermann::Provider::RDKafka::Producer).to receive(:new).with(brokers.first)
        expect(producer).to be_a Hermann::Producer
      end
    end

    context 'with Java', :platform => :java do
      it 'joins broker array' do
        expect(Hermann::Provider::JavaProducer).to receive(:new).with(brokers.first, opts)
        expect(producer).to be_a Hermann::Producer
      end
    end
  end

  describe '#create_result' do
    subject { producer.create_result }

    it { should be_instance_of Hermann::Result }

    it 'should add the result to the producers children' do
      expect(producer.children).to be_empty
      expect(subject).to be_instance_of Hermann::Result
      expect(producer.children).to_not be_empty
    end
  end

  describe '#push' do
    let(:msg)   { 'foo' }
    let(:passed_topic) { 'bar' }
    let(:partition_key) { 'syncml_shard_68_master' }

    context 'without topic passed' do
      it 'uses initialized topic' do
        expect(producer.internal).to receive(:push_single).with(msg, topic, anything, anything)
        producer.push(msg)
      end
    end
    context 'without topic passed', :platform => :java do
      it 'uses initialized topic and does not have a partition key' do
        expect(producer.internal).to receive(:push_single).with(msg, topic, nil, anything)
        producer.push(msg)
      end
    end
    context 'with topic passed' do
      it 'can change topic' do
        expect(producer.internal).to receive(:push_single).with(msg, passed_topic, anything, anything)
        producer.push(msg, :topic => passed_topic)
      end

      context 'and an array of messags' do
        it 'should propagate the topic' do
          messages = 3.times.map { |i| msg }
          expect(producer.internal).to receive(:push_single).with(msg, passed_topic, anything, anything).exactly(messages.size).times
          producer.push(messages, :topic => passed_topic)
        end
      end
    end

    context 'with explicit partition key' do
      it 'uses the partition key' do
        expect(producer.internal).to receive(:push_single).with(msg, topic, partition_key, anything)
        producer.push(msg, :partition_key => partition_key)
      end
    end

    context 'when reaping children', :platform => :java do
      subject { producer.push('f', :topic => passed_topic) }

      context 'with reapable children' do
        it 'should reap the children' do
          promise = Concurrent::Promise.execute { 'f' }.wait(1)
          producer.instance_variable_set(:@children, [promise])
          expect{subject}.to change{producer.children.size}.by(0)
        end
      end

      context 'with no reapable children' do
        it 'should not reap the children' do
          promise = Concurrent::Promise.new {'f'}
          producer.instance_variable_set(:@children, [promise])
          expect{subject}.to change{producer.children.size}.by(1)
        end
      end
    end
  end

  describe '#create_result' do
    subject { producer.create_result }

    it { should be_instance_of Hermann::Result }

    it 'should add the result to the producers children' do
      expect(producer.children).to be_empty
      expect(subject).to be_instance_of Hermann::Result
      expect(producer.children).to_not be_empty
    end
  end

  describe '#connected?' do
    subject { producer.connected? }
    context 'by default' do
      before :each do
        expect(producer.internal).to receive(:connected?).and_call_original
      end

      it { should be false  }
    end
  end

  describe '#connect' do
    let(:timeout) { 0 }
    subject(:connect!) { producer.connect(timeout) }

    it 'should delegate connection to the underlying Producer' do
      expect(producer.internal).to receive(:connect).and_call_original
      connect!
    end
  end

  context "on C ruby", :platform => :mri do
    describe '#push' do
      subject(:result) { producer.push(value) }

      context 'error conditions' do
        shared_examples 'an error condition' do
          it 'should raise an exception' do
            expect { producer.push('rspec') }.to raise_error
          end
        end

        context 'with a bad broker configuration' do
          let(:brokers) { '' }
          it_behaves_like 'an error condition'
        end

        context 'with a non-existing broker' do
          let(:brokers) { ['localhost:13337'] }
          let(:timeout) { 2 }
          let(:value) { 'rspec' }

          it 'should reject' do
            future = result
            expect(future).not_to be_nil
            producer.tick_reactor(timeout)
            expect(future).to be_rejected
          end
        end

        context 'with a bad topic' do
          let(:topic) { '' }
          it_behaves_like 'an error condition'
        end
      end

      context 'with a single value' do
        let(:value) { 'hello' }

        it 'should invoke #push_single' do
          expect(producer.internal).to receive(:push_single)
          expect(result).to be_instance_of Hermann::Result
        end
      end

      context 'with an array value' do
        let(:value) { ['hello', 'world'] }

        it 'should invoke #push_single for each element' do
          value.each do |v|
            expect(producer.internal).to receive(:push_single).with(v, topic, anything, anything)
          end

          expect(result).to be_instance_of Array
          result.each do |elem|
            expect(elem).to be_instance_of Hermann::Result
          end
        end
      end
    end

    describe '#tick_reactor' do
      let(:timeout) { 0 }
      let(:internal) { double('Hermann::Provider::RDKafka::Producer mock') }
      subject(:tick) { producer.tick_reactor(timeout) }

      before :each do
        3.times do
          child = Hermann::Result.new(producer)
          allow(child).to receive(:completed?) { reap }
          producer.children << child
        end

        producer.instance_variable_set(:@internal, internal)
        expect(internal).to receive(:tick)
      end

      context 'with no reapable children' do
        let(:reap) { false }

        it 'should not reap the children' do
          count = producer.children.size
          expect(tick).to eql(0)
          expect(producer.children.size).to eql(count)
        end
      end

      context 'with reapable children' do
        let(:reap) { true }

        it 'should not reap the children' do
          count = producer.children.size
          expect(tick).to eql(count)
          expect(producer.children.size).to_not eql(count)
        end
      end
    end
  end
end

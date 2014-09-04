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
        expect(result).to be_instance_of Hermann::Result
      end
    end

    context 'with an array value' do
      let(:value) { ['hello', 'world'] }

      it 'should invoke #push_single for each element' do
        value.each do |v|
          expect(producer.internal).to receive(:push_single).with(v, anything)
        end

        expect(result).to be_instance_of Array
        result.each do |elem|
          expect(elem).to be_instance_of Hermann::Result
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

  describe '#tick_reactor' do
    let(:timeout) { 0 }
    let(:internal) { double('Hermann::Lib::Producer mock') }
    subject(:tick) { producer.tick_reactor(timeout) }

    before :each do
      3.times do
        child = Hermann::Result.new(producer)
        allow(child).to receive(:reap?) { reap }
        producer.children << child
      end

      producer.instance_variable_set(:@internal, internal)
      expect(internal).to receive(:tick)
    end

    context 'with no reapable children' do
      let(:reap) { false }

      it 'should not reap the children' do
        count = producer.children.size
        expect(tick).to be_nil
        expect(producer.children.size).to eql(count)
      end
    end

    context 'with reapable children' do
      let(:reap) { true }

      it 'should not reap the children' do
        count = producer.children.size
        expect(tick).to be_nil
        expect(producer.children.size).to_not eql(count)
      end
    end
  end
end

require 'spec_helper'
require 'hermann/discovery/zookeeper'
require 'hermann/errors'

describe Hermann::Discovery::Zookeeper do
  let(:zk) { double }
  let(:zookeepers) { 'zk:2181' }
  let(:node) { {'host'=>'f', 'port'=>1 }.to_json }

  subject { described_class.new(zookeepers) }

  describe '#get_brokers' do
    let(:broker_array) do
      [
        JSON.dump({:host => 'f', :port => 1}),
        JSON.dump({:host => 'g', :port => 2}),
      ]
    end

    before do
      impl = double('ZK underlying impl')
      allow(subject).to receive(:impl).and_return(impl)
      expect(impl).to receive(:brokers).and_return(broker_array)
    end

    context 'with valid brokers' do
      it 'gets valid string' do
        expect(subject.get_brokers).to eq ['f:1', 'g:2']
      end
    end

    context 'with no brokers' do
      let(:broker_array) { [] }
      it 'raises an error' do
        expect{ subject.get_brokers }.to raise_error(Hermann::Errors::NoBrokersError)
      end
    end
  end

  # Not implementing many tests here on purpose, the use of the Curator
  # libraries are relatively straight-forward and adding unit tetss to the
  # CuratorImpl doesn't seem worth the trouble of getting the curator libraries
  # properly loaded into the RSpec runtime
  describe Hermann::Discovery::Zookeeper::CuratorImpl do
    subject { described_class }
    it { should respond_to :usable? }
  end

  describe Hermann::Discovery::Zookeeper::ZkGemImpl do
    context 'class methods' do
      subject { described_class }
      it { should respond_to :usable? }
    end

    context 'instance methods' do
      subject { described_class.new(zookeepers) }

      describe '#brokers' do
        let(:broker_ids) { [1] }
        it 'fetches the formatted broker list' do
          allow(zk).to receive(:children).with(any_args) { broker_ids }
          allow(subject).to receive(:fetch_znode).with(any_args) { node }
          expect(subject.send(:fetch_brokers, zk)).to be_instance_of Array
        end
      end

      describe '#fetch_znode' do
        let(:id) { 1 }
        let(:result) { ['foo'] }
        it 'fetches the znode from zookeeper' do
          allow(zk).to receive(:get).with(any_args) { result }
          expect(subject.send(:fetch_znode, zk, id)).to eq result.first
        end

        it 'returns nil node not found' do
          expect(described_class.usable?).to be(true)
          allow(zk).to receive(:get).and_raise(ZK::Exceptions::NoNode)
          expect(subject.send(:fetch_znode, zk, id)).to be_nil
        end
      end
    end
  end

  describe '#format_broker_from_znode' do
    let(:partial_znode) { {'host'=>'f' }.to_json }

    it 'formats the broker string from znode' do
      expect(subject.send(:format_broker_from_znode, node)).to eq 'f:1'
    end

    it 'returns nil if node not found' do
      allow(JSON).to receive(:parse).and_raise(JSON::ParserError)
      expect(subject.send(:format_broker_from_znode, node)).to be_nil
    end

    it 'returns nil if node not complete' do
      expect(subject.send(:format_broker_from_znode, partial_znode)).to be_nil
    end
  end
end


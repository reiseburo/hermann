require 'spec_helper'
require 'hermann/discovery/zookeeper'
require 'hermann/errors'

describe Hermann::Discovery::Zookeeper do
  let(:zk) { double }
  let(:zookeepers) { double }
  let(:node) { {'host'=>'f', 'port'=>1 }.to_json }

  subject { described_class.new(zookeepers) }

  describe '#get_brokers' do
    let(:broker_array) { ['f:1','a:2'] }
    before do
      allow(ZK).to receive(:open).with(any_args).and_yield(zk)
      allow(subject).to receive(:fetch_brokers).with(any_args) { brokers }
    end
    context 'with valid brokers' do
      let(:brokers) { broker_array }
      it 'gets valid string' do
        expect(subject.get_brokers).to eq broker_array
      end
    end
    context 'with no brokers' do
      let(:brokers) { [] }
      it 'raises an error' do
        expect{ subject.get_brokers }.to raise_error(Hermann::Errors::NoBrokersError)
      end
    end
  end

  describe '#fetch_brokers' do
    let(:broker_ids) { [1] }
    it 'fetches the formatted broker list' do
      allow(zk).to receive(:children).with(any_args) { broker_ids }
      allow(subject).to receive(:fetch_znode).with(any_args) { node }
      expect(subject.send(:fetch_brokers, zk)).to eq ['f:1']
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
      allow(zk).to receive(:get).and_raise(ZK::Exceptions::NoNode)
      expect(subject.send(:fetch_znode, zk, id)).to be_nil
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


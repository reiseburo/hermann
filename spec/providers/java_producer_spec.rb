require 'spec_helper'
require 'hermann/provider/java_producer'

describe Hermann::Provider::JavaProducer, :platform => :java  do
  subject(:producer) { described_class.new(topic, zookeepers) }

  let(:topic)      { 'rspec' }
  let(:zookeepers) { 'localhost:2181' }
  let(:brokers)    { '0:1337'}

  describe '#broker_list' do
    let(:val) { ["host"=>"f", "port"=>"1"] }
    it 'gets the list of brokers' do
      allow_any_instance_of(described_class).to receive(:get_all_zookeepers_brokers).with(zookeepers) { val }
      expect(producer.broker_list(zookeepers)).to eq "f:1"
    end
  end

  describe '#get_all_zookeepers_brokers' do
    let(:zk) { 'foo' }
    it 'gets all the brokers' do
      allow_any_instance_of(described_class).to receive(:broker_list) { brokers }
      allow(ZK).to receive(zookeepers).and_yield(zk)
      expect(producer).to receive(:get_zookeeper_brokers).with(any_args)
      producer.get_all_zookeepers_brokers(zookeepers)
    end
  end

  describe '#get_zookeeper_brokers' do
    let(:zookeeper) { double }
    let(:json) { "{\"jmx_port\":1,\"timestamp\":\"1\",\"host\":\"1.1.1.1\",\"version\":1,\"port\":9}" }
    let(:result) { [{"jmx_port"=>1, "timestamp"=>"1", "host"=>"1.1.1.1", "version"=>1, "port"=>9}] }
    it 'gets hash of brokers' do
      allow_any_instance_of(described_class).to receive(:broker_list) { brokers }
      allow(zookeeper).to receive(:children).with(any_args) { [1] }
      allow(zookeeper).to receive(:get) { [json] }
      expect(producer.get_zookeeper_brokers(zookeeper)).to eq result
    end
  end

  describe '#push_single' do
    subject(:result) { producer.push_single('foo') }

    before do
      allow_any_instance_of(described_class).to receive(:broker_list) { brokers }
    end


    it 'returns an executing Promise' do
      expect(result.wait(1).pending?).to eq false
    end

    context 'error conditions' do
      shared_examples 'an error condition' do
        it 'should be rejected' do
          promise = producer.push_single('rspec').wait(1)
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

require 'spec_helper'
require 'hermann_lib'

describe Hermann::Lib::Producer do
  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }
  subject(:producer) { described_class.new(topic, brokers) }
  let(:timeout) { 3000 }

  it { should respond_to :push_single }

  describe '#connect', :type => :integration do
    subject(:connect) { producer.connect(timeout) }

    it 'should connect' do
      expect(connect).to be true
      expect(producer).to be_connected
    end

    context 'with an non-existing broker' do
      let(:brokers) { 'localhost:13337' }

      it 'should attempt to connect' do
        expect(connect).to be false
        expect(producer).not_to be_connected
      end
    end
  end

  describe '#errored?' do
    subject { producer.errored? }

    context 'by default' do
      it { should be false }
    end

    context 'with an non-existing broker' do
      let(:brokers) { 'localhost:13337' }

      it 'should error after attempting to connect' do |example|
        producer.push_single(example.full_description, nil)
        producer.tick(timeout)
        expect(producer).to be_errored
      end
    end
  end

  describe '#connected?' do
    subject { producer.connected? }

    context 'by default' do
      it { should be false }
    end
  end

  describe '#push_single', :type => :integration do
    subject(:push) { |example| producer.push_single(example.full_description, nil) }

    it 'should return' do
      expect(push).not_to be_nil
      producer.tick(timeout)
      expect(producer).to be_connected
    end
  end

  describe '#tick' do
    let(:timeout) { 0 }
    subject(:result) { producer.tick(timeout) }

    context 'with a nil timeout' do
      let(:timeout) { nil }

      it 'should raise an error' do
        expect {
          result
        }.to raise_error(ArgumentError)
      end
    end

    context 'with no requests' do
      it 'should raise an error' do
        expect {
          result
        }.to raise_error
      end
    end

    context 'with a single queued request' do
      before :each do
        producer.push_single('hello', nil)
      end

      it 'should return successfully' do
        expect(result).not_to be_nil
      end
    end
  end
end

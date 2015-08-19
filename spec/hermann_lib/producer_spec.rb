require 'spec_helper'

describe 'Hermann::Provider::RDKafka::Producer', :platform => :mri do
  before :all do
    require 'hermann_rdkafka'
  end

  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }
  subject(:producer) { Hermann::Provider::RDKafka::Producer.new(brokers) }
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
        producer.push_single(example.full_description, 'test-topic', '', nil)
        begin
          producer.tick(timeout)
        rescue StandardError => ex
          # swallow exceptions, since we're just testing #errored?
        end
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
    let(:message) { |example| example.full_description }
    subject(:push) { producer.push_single(message, topic, '', nil) }

    it 'should return' do
      expect(push).not_to be_nil
      producer.tick(timeout)
      expect(producer).to be_connected
    end

    context 'with binary data' do
      let(:message) { "\n+AuOzetQrTrdwSY14ig7I_1oUwjp3DvTx3YWhSTGD4Fo\022\0312014-09-10T00:18:47-07:00\032,\n\006scream\022\016missing_device\032\022flexd-today-0-app0\"\t\n\astarted*(\b\000\022$009f0305-b50a-455d-b137-e52b45f674aa*(\b\001\022$53c0d817-d94b-4b7a-9a58-95fe8cec4333" }

      it 'should return' do
        expect(push).not_to be_nil
        producer.tick(timeout)
        expect(producer).to be_connected
      end
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
        producer.push_single('hello', topic, '', nil)
      end

      it 'should return successfully' do
        expect(result).not_to be_nil
      end
    end
  end
end

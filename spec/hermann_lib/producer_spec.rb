require 'spec_helper'
require 'hermann_lib'

describe Hermann::Lib::Producer do
  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }
  subject(:producer) { described_class.new(topic, brokers) }

  it { should respond_to :push_single }

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

    # NOTE: Not using :type => :integration here to ensure that
    # rd_kafka_poll() has something to do underneath the covers. Successfull
    # pushes return too quickly :)
    ##########################################################################
    context 'with a single queued request' do
      before :each do
        producer.push_single('hello', nil)
      end

      it 'should return successfully' do
        expect(result).to eql(producer)
      end

      context 'with a zero timeout' do
        it 'should not block on #tick' do
          start = Time.now.to_i
          expect(result).to eql(producer)
          finish = Time.now.to_i
          expect(finish - start).to be < 1
        end
      end

      context 'with a non-zero timeout' do
        let(:timeout) { 2000 }
        let(:brokers) { 'localhost:1337' }
        it 'should block #tick' do
          start = Time.now.to_i
          expect(result).to eql(producer)
          finish = Time.now.to_i
          expect(finish - start).to be > 1
        end
      end
    end
    ##########################################################################
  end
end

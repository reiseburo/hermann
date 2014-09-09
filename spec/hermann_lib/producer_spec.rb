require 'spec_helper'
require 'hermann_lib'

describe Hermann::Lib::Producer do
  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }
  subject(:producer) { described_class.new(topic, brokers) }

  it { should respond_to :push_single }
  it { should respond_to :tick }

  describe '#tick', :type => :integration do
    let(:timeout) { 0 }
    subject(:result) { producer.tick(timeout) }

    context 'with no requests' do
      it 'should raise an error' do
        expect {
          result
        }.to raise_error
      end
    end
  end
end

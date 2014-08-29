require 'spec_helper'
require 'hermann/consumer'

describe Hermann::Consumer do
  subject(:consumer) { described_class.new(topic, brokers, partition) }

  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }
  let(:partition)  { 1 }

  it { should respond_to :consume }

  describe '#consume' do
    # XXX: MUST BE FIXED
    #context 'with a bad partition' do
    #  let(:partition) { -1 }

    #  it 'should raise an exception' do
    #    consumer.consume
    #  end
    #end
  end
end

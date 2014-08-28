require 'spec_helper'

describe Hermann::Producer do
  subject(:producer) { described_class.new(topic, brokers) }

  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }

  describe '#push' do
    it { should respond_to :push }
  end
end

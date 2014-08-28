require 'spec_helper'

describe Hermann::Consumer do
  subject(:consumer) { described_class.new(topic, brokers, partition) }

  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }
  let(:partition)  { 1 }

  it { should respond_to :consume }
end

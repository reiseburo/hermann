require 'spec_helper'
require 'hermann_lib'

describe Hermann::Lib::Producer do
  let(:topic) { 'rspec' }
  let(:brokers) { 'localhost:1337' }
  subject(:producer) { described_class.new(topic, brokers) }

  it { should respond_to :push_single }
  it { should respond_to :tick }
end

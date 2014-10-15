require 'spec_helper'
require 'hermann/result'

describe Hermann::Result do
  let(:producer) { double('Mock Hermann::Producer') }
  subject(:result) { described_class.new(producer) }

  describe '#value' do
    let(:timeout) { 0 }
    subject { result.value(timeout) }

    before :each do
      # We cannot resolve a value unless we've ticked the reactor at least once
      expect(producer).to receive(:tick_reactor).with(timeout)
    end

    context 'by default' do
      it { should be_nil }
    end

    context 'after a value has been set internally' do
      let(:value) { 'rspec-payload-value' }
      before(:each) { result.internal_set_value(value, false) }
      it { should eql(value) }
    end
  end

  describe '#completed?' do
    subject { result.completed? }

    context 'if state == :pending' do
      before(:each) { allow(result).to receive(:pending?) { true } }
      it { should be false }
    end

    context 'if state == :unfulfilled' do
      before(:each) { allow(result).to receive(:unfulfilled?) { true } }
      it { should be false }
    end

    context 'if state == :fulfilled' do
      before(:each) { allow(result).to receive(:fulfilled?) { true } }
      it { should be true}
    end

    context 'if state == :rejected' do
      before(:each) { allow(result).to receive(:rejected?) { true } }
      it { should be true}
    end
  end

  describe '#rejected?' do
    subject { result.rejected? }

    context' by default' do
      it { should be false }
    end
  end

  describe '#pending?' do
    subject { result.pending? }

    context' by default' do
      it { should be false }
    end
  end
end

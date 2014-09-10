require 'spec_helper'
require 'hermann/timeout'

# Forward declare the module so this will work even without SystemTimer
# installed (e.g. non-1.8.7)
module SystemTimer; end

describe Hermann::Timeout do
  describe '#timeout' do
    let(:seconds) { 5 }
    let(:result) { 'rspec' }
    subject(:timeout!) { described_class.timeout(seconds) { result } }

    context 'with system_timer' do
      before :each do
        expect(described_class).to receive(:system_timer?).and_return(true)
        expect(SystemTimer).to receive(:timeout_after).and_return(result)
      end

      it { should eql(result) }
    end

    context 'without system_timer' do
      before :each do
        expect(described_class).to receive(:system_timer?).and_return(false)
      end

      it { should eql(result) }
    end
  end
end

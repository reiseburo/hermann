require 'spec_helper'
require 'hermann/errors'

describe Hermann do
  it { should be_instance_of Module }

  describe '.validate_property!' do
    subject { described_class.send(:validate_property!, foo, bar) }

    context 'with valid property' do
      let(:foo) { 'foo' }
      let(:bar) { 'bar' }
      it 'returns true' do
        expect{ subject }.to_not raise_error
      end
    end

    context 'with valid property' do
      let(:foo) { '' }
      let(:bar) { '' }
      it 'returns false' do
        expect{ subject }.to raise_error(Hermann::Errors::ConfigurationError)
      end
    end
  end

  context 'on Jruby', :platform => :java do
    describe '.package_properties' do
      let(:options) { { 'f' => '1' } }
      it 'puts options into java Properties' do
        expect(described_class.package_properties(options)).to eq options
      end
    end
  end
end


##
# This file is auto-generated. DO NOT EDIT!
#
require 'protobuf/message'

module Hermann

  ##
  # Enum Classes
  #
  class States < ::Protobuf::Enum
    define :FULFILLED, 1
    define :UNFULFILLED, 2
    define :PENDING, 3
    define :REJECTED, 4
  end


  ##
  # Message Classes
  #
  class TestEvent < ::Protobuf::Message; end


  ##
  # Message Fields
  #
  class TestEvent
    required :string, :name, 1
    required ::Hermann::States, :state, 2
    optional :int32, :bogomips, 3
  end

end


# Produce messages for a given amount of time

require 'rubygems'
require 'hermann'

stopTime = Time.now + 60 # One minute from now
p = Hermann::Producer.new("lms_messages")
count = 0
while(Time.now < stopTime)
  p.push("Message_#{count}")
  count = count + 1
end
puts("Done!")
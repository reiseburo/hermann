require 'rubygems'
require 'lib/hermann'
require 'lib/hermann/consumer'

p = Hermann::Producer.new("lms_messages", "localhost:9092")
arr = (0..1000000).to_a.map { |x| "message_#{x}"}
t1 = Time.now
arr.each { |x| p.push(x) }
t2 = Time.now
elapsed = t2 - t1
puts "Done!"
puts "Total elapsed time: #{elapsed} seconds"
sleep 30

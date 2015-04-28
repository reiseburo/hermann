require 'rubygems'

$LOAD_PATH << File.dirname(__FILE__) + "/../lib"
$LOAD_PATH << File.dirname(__FILE__) + "/../ext"
require 'hermann'
require 'hermann/consumer'

t1 = 0
threads = []
100.times do |i|
  threads << Thread.new do
    puts "booting #{i}"
    c = Hermann::Consumer.new( "maxwell", brokers: "localhost:9092", partition: i, offset: :start)
    c.consume() do
      |msg| puts("Received: #{msg}")
      if(t1 == 0)
        t1 = Time.now
      end
      t2 = Time.now
      elapsed = t2 - t1
      puts("Total elapsed time: #{elapsed} seconds")
    end
  end
end
threads.each(&:join)

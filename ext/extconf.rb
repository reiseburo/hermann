# External configuration for Hermann Gem

require 'mkmf'

RbConfig::MAKEFILE_CONFIG['CC'] = ENV['CC'] if ENV['CC']

LIBDIR      = RbConfig::CONFIG['libdir']
INCLUDEDIR  = RbConfig::CONFIG['includedir']

puts "Library Dir: #{LIBDIR}\n"
puts "Include Dir: #{INCLUDEDIR}"

HEADER_DIRS = [INCLUDEDIR]

LIB_DIRS = [LIBDIR]

dir_config('rdkafka', HEADER_DIRS, LIB_DIRS)

unless find_header('librdkafka/rdkafka.h')
  abort "librdkafka not installed"
end

unless find_library('rdkafka', 'rd_kafka_conf_new')
   abort "librdkafka not installed"
end

# create_header('hermann_lib.h')
create_makefile('hermann/hermann_lib')
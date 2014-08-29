# External configuration for Hermann Gem

require 'mkmf'

RbConfig::MAKEFILE_CONFIG['CC'] = ENV['CC'] if ENV['CC']

LIBDIR      = RbConfig::CONFIG['libdir']
INCLUDEDIR  = RbConfig::CONFIG['includedir']

puts "Library Dir: #{LIBDIR}\n"
puts "Include Dir: #{INCLUDEDIR}"

HEADER_DIRS = [INCLUDEDIR]

LIB_DIRS = [LIBDIR, './librdkafka']

dir_config('rdkafka', HEADER_DIRS, LIB_DIRS)

ROOTDIR = File.expand_path(File.dirname(__FILE__) + '/../../')
RDK = File.join(ROOTDIR, 'librdkafka/target')
unless find_header('librdkafka/rdkafka.h', File.join(RDK, 'include'))
  abort "librdkafka not installed"
end

unless find_library('rdkafka', 'rd_kafka_conf_new', File.join(RDK, 'lib'))
   abort "librdkafka not installed"
end

#unless have_func "rb_thread_blocking_region"
#  abort "rb_thread_blocking_region function missing"
#end

# create_header('hermann_lib.h')
create_makefile('hermann/hermann_lib')

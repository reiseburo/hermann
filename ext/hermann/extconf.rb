# External configuration for Hermann Gem

require 'rubygems'
require 'mkmf'
require 'mini_portile'
require 'digest/md5'

RbConfig::MAKEFILE_CONFIG['CC'] = ENV['CC'] if ENV['CC']

LIBDIR      = RbConfig::CONFIG['libdir']
INCLUDEDIR  = RbConfig::CONFIG['includedir']
BASE_DIR = File.expand_path(File.dirname(__FILE__) + '/../../')

puts "Library Dir: #{LIBDIR}\n"
puts "Include Dir: #{INCLUDEDIR}"

################################################################################
#   MiniPortile overrides
################################################################################
# RdKafkaRecipe is a class that adds some librdkafka specific customizations to
# make sure that we can safely build librdkafka when installing this gem
class RdKafkaRecipe < MiniPortile
  attr_accessor :checksum

  def configure
    execute('configure', %Q(bash configure #{computed_options}))
  end

  def configured?
    File.exists?(File.join(work_path, 'Makefile.config'))
  end

  # Overriding this from MiniPortile because it includes autoconf defaults that
  # don't apply to librdkafka's mklove-based configure script
  def configure_defaults
    []
  end

  def download_file(url, full_path, count=3)
    super(url, full_path, count)

    # Support some simple checksumming
    unless Digest::MD5.hexdigest(File.read(full_path)) == checksum
      raise 'Checksum error!'
    end
  end
end
################################################################################

librdkafka = RdKafkaRecipe.new('librdkafka', '0.8.4')
librdkafka.files = ["https://github.com/edenhill/librdkafka/archive/#{librdkafka.version}.tar.gz"]
librdkafka.checksum = '28a3252fd0f31d4a38bea9cd25083a06'
librdkafka.patch_files = Dir["#{File.join(BASE_DIR, 'ext', 'patches', 'librdkafka')}/*.patch"]
checkpoint = ".librdkafka.#{librdkafka.version}.cooked"

unless File.exists?(checkpoint)
  librdkafka.cook
  File.open(checkpoint, 'w+') do |f|
    f.write("Cooked: #{Time.now}\n")
  end
end

librdkafka.activate

HEADER_DIRS = [INCLUDEDIR, File.join(librdkafka.path, 'include')]
LIB_DIRS = [LIBDIR]
dir_config('rdkafka', HEADER_DIRS, LIB_DIRS)

# Tell mkmf to staticallly link our mini_portile generated static library,
# courtesty of:
#   <http://blog.zachallett.com/howto-ruby-c-extension-with-a-static-library>
$LOCAL_LIBS << File.join(librdkafka.path, 'lib', 'librdkafka.a')

create_makefile('hermann/hermann_lib')

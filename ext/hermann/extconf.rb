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

  def download_file_http(url, full_path, count = 3)
    filename = File.basename(full_path)
    uri = URI.parse(url)

    if ENV['http_proxy']
      _, userinfo, p_host, p_port = URI.split(ENV['http_proxy'])
      proxy_user, proxy_pass = userinfo.split(/:/) if userinfo
      http = Net::HTTP.new(uri.host, uri.port, p_host, p_port, proxy_user, proxy_pass)
    else
      http = Net::HTTP.new(uri.host, uri.port)

      if URI::HTTPS === uri
        http.use_ssl = true
        http.verify_mode = OpenSSL::SSL::VERIFY_PEER

        store = OpenSSL::X509::Store.new

        # Auto-include system-provided certificates
        store.set_default_paths

        if ENV.has_key?("SSL_CERT_FILE") && File.exist?(ENV["SSL_CERT_FILE"])
          store.add_file ENV["SSL_CERT_FILE"]
        end

        http.cert_store = store
      end
    end

    message "Downloading #{filename} "
    http.start do |h|
      h.request_get(uri.path, 'Accept-Encoding' => 'identity') do |response|
        case response
        when Net::HTTPNotFound
          output "404 - Not Found"
          return false

        when Net::HTTPClientError
          output "Error: Client Error: #{response.inspect}"
          return false

        when Net::HTTPRedirection
          raise "Too many redirections for the original URL, halting." if count <= 0
          url = response["location"]
          return download_file(url, full_path, count - 1)

        when Net::HTTPOK
          return with_tempfile(filename, full_path) do |temp_file|
            size = 0
            progress = 0
            total = response.header["Content-Length"].to_i

            if total == 0
              # There are cases when apparently GitHub.com will return an empty
              # content-length header, which means we can't really trust the
              # response, so we'll treat it like a redirect
              puts "Empty content-length header, retrying"
              return download_file(url, full_path, count - 1)
            end

            response.read_body do |chunk|
              temp_file << chunk
              size += chunk.size
              new_progress = (size * 100) / total
              unless new_progress == progress
                message "\rDownloading %s (%3d%%) " % [filename, new_progress]
              end
              progress = new_progress
            end
            output
          end
        end
      end
    end
  end
end
################################################################################

librdkafka = RdKafkaRecipe.new('librdkafka', '0.8.6')
librdkafka.files = ["https://github.com/edenhill/librdkafka/archive/#{librdkafka.version}.tar.gz"]
librdkafka.checksum = '1b77543f9be82d3f700c0ef98f494990'
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

have_header('ruby/thread.h')
have_header('ruby/intern.h')
have_header('ruby/version.h')
have_func('rb_thread_blocking_region')
have_func('rb_thread_call_without_gvl')

create_makefile('hermann/hermann_rdkafka')

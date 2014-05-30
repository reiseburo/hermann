
require 'rake/clean'

EXT_CONF = "ext/extconf.rb"
MAKEFILE = 'ext/Makefile'
MODULE = 'ext/hermann_lib.so'
SRC = Dir.glob('ext/*.c')
SRC << MAKEFILE

CLEAN.include [ 'ext/*.o', 'ext/depend', 'ext/hermann_lib.bundle', MODULE ]
CLOBBER.include [ 'config.save', 'ext/mkmf.log', 'ext/hermann_lib.bundle', MAKEFILE ]

file MAKEFILE => EXT_CONF do |t|
  Dir::chdir(File::dirname(EXT_CONF)) do
    unless sh "ruby #{File::basename(EXT_CONF)}"
      $stderr.puts "Failed to run extconf"
      break
    end
  end
end

file MODULE => SRC do |t|
  Dir::chdir(File::dirname(EXT_CONF)) do
    unless sh "make"
      $stderr.puts "make failed"
    end
  end
end

desc "Build the native library"
task :build => MODULE
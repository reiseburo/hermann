
SHELL = /bin/sh

#### Start of system configuration section. ####

srcdir = .
topdir = /Users/scampbell/.rbenv/versions/1.8.7-p375/lib/ruby/1.8/i686-darwin13.2.0
hdrdir = $(topdir)
VPATH = $(srcdir):$(topdir):$(hdrdir)
prefix = $(DESTDIR)/Users/scampbell/.rbenv/versions/1.8.7-p375
exec_prefix = $(prefix)
sitedir = $(libdir)/ruby/site_ruby
archdir = $(rubylibdir)/$(arch)
sitelibdir = $(sitedir)/$(ruby_version)
vendorlibdir = $(vendordir)/$(ruby_version)
sbindir = $(exec_prefix)/sbin
localedir = $(datarootdir)/locale
datarootdir = $(prefix)/share
includedir = $(prefix)/include
localstatedir = $(prefix)/var
htmldir = $(docdir)
infodir = $(datarootdir)/info
docdir = $(datarootdir)/doc/$(PACKAGE)
pdfdir = $(docdir)
sitearchdir = $(sitelibdir)/$(sitearch)
libexecdir = $(exec_prefix)/libexec
bindir = $(exec_prefix)/bin
libdir = $(exec_prefix)/lib
psdir = $(docdir)
sysconfdir = $(prefix)/etc
oldincludedir = $(DESTDIR)/usr/include
rubylibdir = $(libdir)/ruby/$(ruby_version)
vendordir = $(libdir)/ruby/vendor_ruby
dvidir = $(docdir)
mandir = $(datarootdir)/man
sharedstatedir = $(prefix)/com
datadir = $(datarootdir)
vendorarchdir = $(vendorlibdir)/$(sitearch)

CC = /usr/local/bin/gcc-4.2
LIBRUBY = $(LIBRUBY_A)
LIBRUBY_A = lib$(RUBY_SO_NAME)-static.a
LIBRUBYARG_SHARED = 
LIBRUBYARG_STATIC = -l$(RUBY_SO_NAME)-static

RUBY_EXTCONF_H = 
CFLAGS   =  -fno-common  -O3 -Wno-error=shorten-64-to-32  -pipe -fno-common $(cflags) 
INCFLAGS = -I. -I. -I/Users/scampbell/.rbenv/versions/1.8.7-p375/lib/ruby/1.8/i686-darwin13.2.0 -I.
DEFS     = 
CPPFLAGS =  -I/Users/scampbell/.rbenv/versions/1.8.7-p375/include  -D_XOPEN_SOURCE -D_DARWIN_C_SOURCE  -I/Users/scampbell/.rbenv/versions/1.8.7-p375/include 
CXXFLAGS = $(CFLAGS) 
ldflags  = -L. -L/Users/scampbell/.rbenv/versions/1.8.7-p375/lib 
dldflags = 
archflag = 
DLDFLAGS = $(ldflags) $(dldflags) $(archflag)
LDSHARED = cc -dynamic -bundle -undefined suppress -flat_namespace
AR = ar
EXEEXT = 

RUBY_INSTALL_NAME = ruby
RUBY_SO_NAME = ruby
arch = i686-darwin13.2.0
sitearch = i686-darwin13.2.0
ruby_version = 1.8
ruby = /Users/scampbell/.rbenv/versions/1.8.7-p375/bin/ruby
RUBY = $(ruby)
RM = rm -f
MAKEDIRS = mkdir -p
INSTALL = /usr/bin/install -c
INSTALL_PROG = $(INSTALL) -m 0755
INSTALL_DATA = $(INSTALL) -m 644
COPY = cp

#### End of system configuration section. ####

preload = 

libpath = . $(libdir) /Users/scampbell/.rbenv/versions/1.8.7-p375/lib
LIBPATH =  -L. -L$(libdir) -L/Users/scampbell/.rbenv/versions/1.8.7-p375/lib
DEFFILE = 

CLEANFILES = mkmf.log
DISTCLEANFILES = 

extout = 
extout_prefix = 
target_prefix = /hermann
LOCAL_LIBS = 
LIBS =  -lrdkafka  -ldl -lobjc  
SRCS = hermann_lib.c
OBJS = hermann_lib.o
TARGET = hermann_lib
DLLIB = $(TARGET).bundle
EXTSTATIC = 
STATIC_LIB = 

BINDIR        = $(bindir)
RUBYCOMMONDIR = $(sitedir)$(target_prefix)
RUBYLIBDIR    = $(sitelibdir)$(target_prefix)
RUBYARCHDIR   = $(sitearchdir)$(target_prefix)

TARGET_SO     = $(DLLIB)
CLEANLIBS     = $(TARGET).bundle $(TARGET).il? $(TARGET).tds $(TARGET).map
CLEANOBJS     = *.o *.a *.s[ol] *.pdb *.exp *.bak

all:		$(DLLIB)
static:		$(STATIC_LIB)

clean:
		@-$(RM) $(CLEANLIBS) $(CLEANOBJS) $(CLEANFILES)

distclean:	clean
		@-$(RM) Makefile $(RUBY_EXTCONF_H) conftest.* mkmf.log
		@-$(RM) core ruby$(EXEEXT) *~ $(DISTCLEANFILES)

realclean:	distclean
install: install-so install-rb

install-so: $(RUBYARCHDIR)
install-so: $(RUBYARCHDIR)/$(DLLIB)
$(RUBYARCHDIR)/$(DLLIB): $(DLLIB)
	$(INSTALL_PROG) $(DLLIB) $(RUBYARCHDIR)
install-rb: pre-install-rb install-rb-default
install-rb-default: pre-install-rb-default
pre-install-rb: Makefile
pre-install-rb-default: Makefile
$(RUBYARCHDIR):
	$(MAKEDIRS) $@

site-install: site-install-so site-install-rb
site-install-so: install-so
site-install-rb: install-rb

.SUFFIXES: .c .m .cc .cxx .cpp .C .o

.cc.o:
	$(CXX) $(INCFLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $<

.cxx.o:
	$(CXX) $(INCFLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $<

.cpp.o:
	$(CXX) $(INCFLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $<

.C.o:
	$(CXX) $(INCFLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $<

.c.o:
	$(CC) $(INCFLAGS) $(CPPFLAGS) $(CFLAGS) -c $<

$(DLLIB): $(OBJS) Makefile
	@-$(RM) $@
	$(LDSHARED) -o $@ $(OBJS) $(LIBPATH) $(DLDFLAGS) $(LOCAL_LIBS) $(LIBS)



$(OBJS): ruby.h defines.h

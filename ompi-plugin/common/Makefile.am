AM_CPPFLAGS = -I$(prefix)/include/openmpi

# main library setup
#-----------------
#noinst_LTLIBRARIES = libhamster_common.la
lib_LTLIBRARIES = libhamster_common.la
libhamster_common_la_SOURCES =

# pkgdata setup
dist_pkgdata_DATA = hamster_protos.pb

# local files
headers = hdclient.h
ortedir = $(includedir)/openmpi/$(subdir)
nobase_orte_HEADERS = $(headers)

include base/Makefile.am

distclean-local: 
	rm -f base/static-components.h

install-data-local:
	$(MKDIR_P) $(DESTDIR)$(sysconfdir)/protos/
	$(INSTALL_DATA) hamster_protos.pb $(DESTDIR)$(sysconfdir)/protos/

uninstall-local:
	rm -rf $(DESTDIR)$(sysconfdir)/protos/hamster_protos.pb
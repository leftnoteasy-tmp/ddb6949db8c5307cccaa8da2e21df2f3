AUTOMAKE_OPTIONS = foreign
AM_CPPFLAGS = -I$(prefix)/include/openmpi -I$(prefix)/include/openmpi/opal/mca/event/libevent2019/libevent -I$(prefix)/include/openmpi/opal/mca/hwloc/hwloc152/hwloc/include

sources = \
        odls_yarn.h \
        odls_yarn_component.c \
        odls_yarn_module.c

mcacomponentdir = $(libdir)/openmpi
mcacomponent_LTLIBRARIES = mca_odls_yarn.la
mca_odls_yarn_la_SOURCES = $(sources) 
mca_odls_yarn_la_LIBADD = $(prefix)/lib/libopen-rte.la
mca_odls_yarn_la_LDFLAGS = -module -avoid-version 
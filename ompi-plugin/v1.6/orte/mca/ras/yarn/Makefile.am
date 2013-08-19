AUTOMAKE_OPTIONS = foreign
AM_CPPFLAGS = -I$(prefix)/include/openmpi -I$(prefix)/include/openmpi/opal/mca/event/libevent2019/libevent -I$(prefix)/include/openmpi/opal/mca/hwloc/hwloc152/hwloc/include

sources = \
        ras_yarn.h \
        ras_yarn_component.c \
        ras_yarn_module.c

mcacomponentdir = $(libdir)/openmpi
mcacomponent_LTLIBRARIES = mca_ras_yarn.la
mca_ras_yarn_la_SOURCES = $(sources) 
mca_ras_yarn_la_LIBADD = $(prefix)/lib/libopen-rte.la $(top_builddir)/common/libhamster_common.la
mca_ras_yarn_la_LDFLAGS = -module -avoid-version 
#mca_ras_yarn_la_DEPENDENCIES = $(top_builddir)/common/libhamster_common.la
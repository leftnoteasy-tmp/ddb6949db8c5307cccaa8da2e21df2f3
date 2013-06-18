#
# Copyright (c) 2004-2005 The University of Tennessee and The University
#                         of Tennessee Research Foundation.  All rights
#                         reserved.
# Copyright (c) 2009      High Performance Computing Center Stuttgart, 
#                         University of Stuttgart.  All rights reserved.
# Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

EXTRA_DIST = .windows

dist_pkgdata_DATA = help-odls-yarn.txt

sources = \
        odls_yarn.h \
        odls_yarn_component.c \
        odls_yarn_module.c

# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).

if OMPI_BUILD_odls_yarn_DSO
component_noinst =
component_install = mca_odls_yarn.la
else
component_noinst = libmca_odls_yarn.la
component_install =
endif

mcacomponentdir = $(pkglibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_odls_yarn_la_SOURCES = $(sources)
mca_odls_yarn_la_LDFLAGS = -module -avoid-version

noinst_LTLIBRARIES = $(component_noinst)
libmca_odls_yarn_la_SOURCES =$(sources)
libmca_odls_yarn_la_LDFLAGS = -module -avoid-version

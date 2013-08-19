#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([ompi-plugin], [0.8], [])
AC_CONFIG_SRCDIR([v1.6/orte/mca/odls/yarn/odls_yarn.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([m4])


AM_INIT_AUTOMAKE(1.8 foreign -Wall -Werror)

# Checks for programs.
AC_PROG_CXX
AC_PROG_AWK
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_RANLIB

#if we will create lib
AM_PROG_AR
AC_PROG_LIBTOOL
LT_INIT

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([arpa/inet.h fcntl.h netdb.h netinet/in.h stddef.h stdint.h stdlib.h string.h sys/param.h sys/socket.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT16_T
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_INT8_T
AC_TYPE_SIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_FUNC_STRTOD
AC_CHECK_FUNCS([bzero gethostbyname gethostname gettimeofday memmove memset socket strchr strdup strstr strtol strtoul])


#AC_ARG_WITH(ompi, [--ompi-version[=VERSION]   OpenMPI version, e.g., 1.7.2], [ompi="$ompi_version"], [ompi=""])
#if test "$ompi_version" == "1.7.2"; then
#	AC_MSG_NOTICE([Your input is 1.7.2])
#	ompi=172
#elif test "$ompi_version" == "1.6.4"; then
#	AC_MSG_NOTICE([Your input is 1.6.4])
#	ompi=164
#else
#	AC_MSG_NOTICE([Your input is a wrong OpenMPI version, currently, only 1.7.2 and 1.6.4 are supported.])
#fi
#AC_SUBST(ompi) 
#AC_DEFINE(ompi)
#AM_CONDITIONAL(OMPI_VERSION, test x$ompi = x172)

AC_ARG_WITH([ompi172],
  			[AS_HELP_STRING([--with-ompi172@<:@=ARG@:>@], [with OpenMPI-1.7.2 support @<:@ARG=no@:>@])],
  			[:],
  			[with_ompi172=no])
#AC_DEFUN([OMPI_VERSION],[
#])  
AM_CONDITIONAL([OMPI_172], [test x$with_ompi172 = xyes])


AC_ARG_WITH([ompi164],
  			[AS_HELP_STRING([--with-ompi164@<:@=ARG@:>@], [with OpenMPI-1.6.4 support @<:@ARG=no@:>@])],
  			[:],
  			[with_ompi164=no])
AM_CONDITIONAL([OMPI_164], [test x$with_ompi164 = xyes])

AC_CONFIG_FILES([Makefile
                 common/Makefile
                 v1.6/Makefile
                 v1.6/orte/Makefile
                 v1.6/orte/mca/Makefile
                 v1.6/orte/mca/odls/Makefile
                 v1.6/orte/mca/odls/yarn/Makefile
                 v1.6/orte/mca/plm/Makefile
                 v1.6/orte/mca/plm/yarn/Makefile
                 v1.6/orte/mca/ras/Makefile
                 v1.6/orte/mca/ras/yarn/Makefile
                 v1.7/Makefile
                 v1.7/orte/Makefile
                 v1.7/orte/mca/Makefile
                 v1.7/orte/mca/odls/Makefile
                 v1.7/orte/mca/odls/yarn/Makefile
                 v1.7/orte/mca/plm/Makefile
                 v1.7/orte/mca/plm/yarn/Makefile
                 v1.7/orte/mca/ras/Makefile
                 v1.7/orte/mca/ras/yarn/Makefile
                 v1.7/orte/mca/state/Makefile
                 v1.7/orte/mca/state/yarn/Makefile
                 ])
AC_OUTPUT
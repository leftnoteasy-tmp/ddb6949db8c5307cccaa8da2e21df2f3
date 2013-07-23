# -*- shell-script -*-
#
# Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
#                         University Research and Technology
#                         Corporation.  All rights reserved.
# Copyright (c) 2004-2005 The University of Tennessee and The University
#                         of Tennessee Research Foundation.  All rights
#                         reserved.
# Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
#                         University of Stuttgart.  All rights reserved.
# Copyright (c) 2004-2005 The Regents of the University of California.
#                         All rights reserved.
# Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

# ORTE_CHECK_YARN(prefix, [action-if-found], [action-if-not-found])
# --------------------------------------------------------
AC_DEFUN([ORTE_CHECK_YARN],[
    AC_ARG_WITH([yarn],
                [AC_HELP_STRING([--with-yarn],
                                [Build YARN scheduler component (default: yes)])])

    if test "$with_yarn" = "no" ; then
        orte_check_yarn_happy="no"
    else 
        orte_check_yarn_happy="yes"
    fi
    
    AS_IF([test "$orte_check_yarn_happy" = "yes"], 
          [$2], 
          [$3])
])

/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */
/**
 * @file
 *
 * Resource Allocation (YARN)
 */
#ifndef ORTE_RAS_YARN_H
#define ORTE_RAS_YARN_H

#include "orte_config.h"
#include "orte/mca/ras/ras.h"
#include "orte/mca/ras/base/base.h"

BEGIN_C_DECLS

typedef struct {
    orte_ras_base_component_t super;
    int timeout;
    bool dyn_alloc_enabled;
    char *config_file;
    bool rolling_alloc;
} orte_ras_yarn_component_t;

ORTE_DECLSPEC extern orte_ras_yarn_component_t mca_ras_yarn_component;
ORTE_DECLSPEC extern orte_ras_base_module_t orte_ras_yarn_module;

END_C_DECLS

#endif

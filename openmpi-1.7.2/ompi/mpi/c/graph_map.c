/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */
#include "ompi_config.h"
#include <stdio.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/mca/topo/topo.h"
#include "ompi/memchecker.h"

#if OPAL_HAVE_WEAK_SYMBOLS && OMPI_PROFILING_DEFINES
#pragma weak MPI_Graph_map = PMPI_Graph_map
#endif

#if OMPI_PROFILING_DEFINES
#include "ompi/mpi/c/profile/defines.h"
#endif

static const char FUNC_NAME[] = "MPI_Graph_map";


int MPI_Graph_map(MPI_Comm comm, int nnodes, int indx[], int edges[],
                  int *newrank) 
{
    int err;
    mca_topo_base_module_graph_map_fn_t func;

    MEMCHECKER(
        memchecker_comm(comm);
    );

    /* check the arguments */
    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(comm)) {
            return OMPI_ERRHANDLER_INVOKE (MPI_COMM_WORLD, MPI_ERR_COMM,
                                           FUNC_NAME);
        }
        if (OMPI_COMM_IS_INTER(comm)) {
            return OMPI_ERRHANDLER_INVOKE (comm, MPI_ERR_COMM,
                                           FUNC_NAME);
        }
        if (1 > nnodes || NULL == indx || NULL == edges || NULL == newrank) {
            return OMPI_ERRHANDLER_INVOKE (comm, MPI_ERR_ARG,
                                           FUNC_NAME);
        }
    }

    OPAL_CR_ENTER_LIBRARY();

    if(!OMPI_COMM_IS_GRAPH(comm)) {
        /* In case the communicator has no topo-module attached to 
           it, we just return the "default" value suggested by MPI:
           newrank = rank */
        *newrank = ompi_comm_rank(comm);
    }
    else {
        /* map the function pointer to do the right thing */
        func = comm->c_topo->topo_graph_map;
	
        /* call the function */
        if ( MPI_SUCCESS != 
             (err = func(comm, nnodes, indx, edges, newrank))) {
            OPAL_CR_EXIT_LIBRARY();
            return OMPI_ERRHANDLER_INVOKE(comm, err, FUNC_NAME);
        }
    }

    /* All done */
    OPAL_CR_EXIT_LIBRARY();
    return MPI_SUCCESS;
}
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2012 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013 Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "orte_config.h"
#include "orte/constants.h"
#include "orte/types.h"

#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/socket.h>
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

#include "opal/util/argv.h"
#include "opal/util/net.h"
#include "opal/util/if.h"
#include "opal/util/output.h"
#include "opal/opal_socket_errno.h"

#include "orte/util/show_help.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/ras/base/ras_private.h"
#include "opal/class/opal_bitmap.h"

#include "ras_yarn.h"

//#include "orte/mca/hdclient/hdclient.h"
#include "common/hdclient.h"

/*
 * API functions
 */
static int init(void);
static int orte_ras_yarn_allocate(opal_list_t *nodes);
static int orte_ras_yarn_finalize(void);


/*
 * Local functions
 */
static int release_containers(int release_cid_array[],  int count);

/*
 * RAS yarn module
 */
orte_ras_base_module_t orte_ras_yarn_module = {
    orte_ras_yarn_allocate,
    orte_ras_yarn_finalize
};

static int init(void)
{
    int rc;
    return ORTE_SUCCESS;
}

static int register_to_am() {
    int rc;
    struct pbc_wmessage* msg = pbc_wmessage_new(orte_hdclient_pb_env, "RegisterRequestProto");
    if (!msg) {
        opal_output(0, "%s ras:yarn failed to create register msg", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    rc = orte_hdclient_send_message_and_delete(msg, HAMSTER_MSG_REGISTER);
    if (rc != 0) {
        opal_output(0, "%s ras:yarn error happened when send register to AM",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    // read rmessage out
    struct pbc_rmessage* response = orte_hdclient_recv_message("RegisterResponseProto");
    if (!response) {
        opal_output(0, "%s ras:yarn error happened when recv register response from AM",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }
    pbc_rmessage_delete(response);

    OPAL_OUTPUT_VERBOSE((5, orte_ras_base.ras_output,
                     "%s ras:yarn successfully registered to AM",
                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    return ORTE_SUCCESS;
}

static int orte_ras_yarn_allocate_internal(int np, opal_list_t* nodes) {
    int rc, i;

    // create and send allocate message
    struct pbc_wmessage* msg = pbc_wmessage_new(orte_hdclient_pb_env, "AllocateRequestProto");
    if (!msg) {
        opal_output(0, "%s ras:yarn failed to create AllocateRequestProto", 
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }
    pbc_wmessage_integer(msg, "resource_count", np, 0);
    rc = orte_hdclient_send_message_and_delete(msg, HAMSTER_MSG_ALLOCATE);
    if (rc != 0) {
        opal_output(0, "%s ras:yarn error happened when send allocate msg to AM",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    // read rmessage out
    struct pbc_rmessage* response = orte_hdclient_recv_message("AllocateResponseProto");
    if (!response) {
        opal_output(0, "%s ras:yarn error happened when recv allocate response from AM",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    int n = pbc_rmessage_size(response, "node_resources");
    if (n <= 0) {
        opal_output(0, "%s ras:yarn got n(=%d) <= 0, please check",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), n);
        return ORTE_ERROR;
    }

    // read node resources
    for (i = 0; i < n; i++) {
        struct pbc_rmessage* node_res = pbc_rmessage_message(response, "node_resources", i);
        if (!node_res) {
            opal_output(0, "%s ras:yarn error when parse returned resource from AM", 
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            return ORTE_ERROR;
        }
        
        // parse host, slot
        const char* host = pbc_rmessage_string(node_res, "host_name", 0, NULL);
        if (!host) {
            opal_output(0, "%s ras:yarn error when parse host from returned resource from AM",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            return ORTE_ERROR;
        }
        int slot = pbc_rmessage_integer(node_res, "slot", 0, NULL);

        // make node_t and add it to nodes
        orte_node_t* node = OBJ_NEW(orte_node_t);
        if (!node) {
            opal_output(0, "%s ras:yarn failed to create orte_node_t obj", 
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            return ORTE_ERROR;
        }

        node->name = strdup(host);
        node->state = ORTE_NODE_STATE_UP;
        node->slots_inuse = 0;
        node->slots_max = 0;
        node->slots = slot;
        opal_list_append(nodes, &node->super);

        OPAL_OUTPUT_VERBOSE((5, orte_ras_base.ras_output,
                     "%s ras:yarn: adding node %s with %d slot",
                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                     host, slot));
    }

    // All done
    OPAL_OUTPUT_VERBOSE((1, orte_ras_base.ras_output,
                         "%s ras:yarn:allocate: success",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    return ORTE_SUCCESS;
}

static int orte_ras_yarn_allocate(opal_list_t* nodes) {
    int i, j, rc;

    // init pb and connect to AM
    if (0 != (rc = orte_hdclient_init_pb_env())) {
        opal_output(0, "%s failed to init pb env",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }
    if (0 != (rc = orte_hdclient_connect_to_am())) {
        opal_output(0, "%s failed to connect to AM",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    // register to am first
    if (ORTE_SUCCESS != (rc = register_to_am())) {
        opal_output(0, "%s error when registering to AM",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    // set managed allocation
    orte_managed_allocation = true;

    // calculate all slots needed
    int slot_num = 0;
    for (i = 1; i < orte_job_data->size; i++) {
        orte_job_t* jdata = opal_pointer_array_get_item(orte_job_data, i);
        if (!jdata) {
            continue;
        }
        orte_app_context_t* app = NULL;
        for (j = 0; j < jdata->apps->size; j++) {
            if (NULL == (app = (orte_app_context_t*)opal_pointer_array_get_item(jdata->apps, j))) {
                continue;
            }
            slot_num += app->num_procs;
        }
    }

    return orte_ras_yarn_allocate_internal(slot_num, nodes);
}

static int orte_ras_yarn_finalize(void) {
    // TODO, add code to close socket, etc.
}

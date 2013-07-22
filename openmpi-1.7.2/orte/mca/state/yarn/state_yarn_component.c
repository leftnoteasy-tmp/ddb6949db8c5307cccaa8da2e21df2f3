/*
 * Copyright (c) 2011      Los Alamos National Security, LLC.
 *                         All rights reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "orte_config.h"
#include "opal/util/output.h"

#include "orte/mca/state/state.h"
#include "orte/mca/state/base/base.h"
#include "state_yarn.h"

/*
 * Public string for version number
 */
const char *orte_state_yarn_component_version_string =
    "ORTE STATE yarn MCA component version " ORTE_VERSION;

/*
 * Local functionality
 */
static int state_yarn_open(void);
static int state_yarn_close(void);
static int state_yarn_component_query(mca_base_module_t **module, int *priority);

/*
 * Instantiate the public struct with all of our public information
 * and pointer to our public functions in it
 */
orte_state_base_component_t mca_state_yarn_component =
{
    /* Handle the general mca_component_t struct containing 
     *  meta information about the component
     */
    {
        ORTE_STATE_BASE_VERSION_1_0_0,
        /* Component name and version */
        "yarn",
        ORTE_MAJOR_VERSION,
        ORTE_MINOR_VERSION,
        ORTE_RELEASE_VERSION,
        
        /* Component open and close functions */
        state_yarn_open,
        state_yarn_close,
        state_yarn_component_query
    },
    {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

static int state_yarn_open(void)
{
    return ORTE_SUCCESS;
}

static int state_yarn_close(void)
{
    return ORTE_SUCCESS;
}

static int state_yarn_component_query(mca_base_module_t **module, int *priority)
{
    if (ORTE_PROC_IS_HNP) {
        /* set our priority high as we are the default for yarns */
        *priority = 30;
        *module = (mca_base_module_t *)&orte_state_yarn_module;
        return ORTE_SUCCESS;        
    }
    
    *priority = -1;
    *module = NULL;
    return ORTE_ERROR;
}

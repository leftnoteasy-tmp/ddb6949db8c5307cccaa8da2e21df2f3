/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics.  Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire components just to query their version and parameters.
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/base/mca_base_param.h"

#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"

#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/base/plm_private.h"
#include "plm_yarn.h"

//#include "opal/util/lt_interface.h"
//#include "opal/libltdl/ltdl.h"
//#include "/Users/caoj7/program/common-c/include/libltdl/lt_system.h"
//#include "/Users/caoj7/program/common-c/include/ltdl.h"


/*
 * Public string showing the plm ompi_yarn component version number
 */
const char *mca_plm_yarn_component_version_string =
  "Open MPI yarn plm MCA component version " ORTE_VERSION;


/*
 * Local functions
 */
static int plm_yarn_open(void);
static int plm_yarn_close(void);
static int orte_plm_yarn_component_query(mca_base_module_t **module, int *priority);


/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

orte_plm_yarn_component_t mca_plm_yarn_component = {

    {
        /* First, the mca_component_t struct containing meta
           information about the component itself */

        {
            ORTE_PLM_BASE_VERSION_2_0_0,
            
            /* Component name and version */
            "yarn",
            ORTE_MAJOR_VERSION,
            ORTE_MINOR_VERSION,
            ORTE_RELEASE_VERSION,
            
            /* Component open and close functions */
            plm_yarn_open,
            plm_yarn_close,
            orte_plm_yarn_component_query
        },
        {
            /* The component is checkpoint ready */
            MCA_BASE_METADATA_PARAM_CHECKPOINT
        }
    }

    /* Other orte_plm_yarn_component_t items -- left uninitialized
       here; will be initialized in plm_yarn_open() */
};


//static int ltdl_pbc()
//{
//
//	int rc;
//	char filename[1024];
//	filename[0] = '\0';
////    opal_lt_dladvise advise;
//	lt_dladvise advise;
//
//
//	opal_output(0, "ltdl_pbc !!!!!!!!!!!!!!!");
////    if (!getenv("OPENMPI_HOME")) {
////        opal_output(0, "plm:yarn:ltdl_pbc: OPENMPI_HOME not set in env, please check.\n");
////        return -1;
////    }
//
//    char* search_path = malloc(1024);
////    strcpy(search_path, getenv("OPENMPI_HOME"));
//    strcpy(search_path, "/Users/caoj7/program/orte");
//    strcat(search_path, "/lib");
//
////    if (opal_lt_dlinit() != 0) {
//    if (lt_dlinit() != 0) {
//        opal_output(0, "LT_DLINIT FAILED - CANNOT LOAD LIBORTE");
//        return -1;
//    }
//
//    /* open the library into the global namespace */
////    if (opal_lt_dladvise_init(&advise)) {
//    if (lt_dladvise_init(&advise)) {
//        opal_output(0, "LT_DLADVISE INIT FAILED - CANNOT LOAD LIBORTE");
//        return -1;
//    }
//
////    if (opal_lt_dladvise_ext(&advise)) {
//    if (lt_dladvise_ext(&advise)) {
//        opal_output(0, "LT_DLADVISE EXT FAILED - CANNOT LOAD LIBORTE");
////        opal_lt_dladvise_destroy(&advise);
//        lt_dladvise_destroy(&advise);
//        return -1;
//    }
//
////    if (opal_lt_dladvise_global(&advise)) {
//    if (lt_dladvise_global(&advise)) {
//        opal_output(0, "LT_DLADVISE GLOBAL FAILED - CANNOT LOAD LIBORTE");
////        opal_lt_dladvise_destroy(&advise);
//        lt_dladvise_destroy(&advise);
//        return -1;
//    }
//
////	opal_lt_dlsetsearchpath(search_path);
//    lt_dlsetsearchpath(search_path);
////	opal_output(2, "dlsearch path: %s.\n", opal_lt_dlgetsearchpath());
//    printf("#####################dlsearch path: %s.\n", lt_dlgetsearchpath());
//    opal_output(0, "#####################dlsearch path: %s.\n", lt_dlgetsearchpath());
//
//	/* load pbc lib */
////	if (!opal_lt_dlopenadvise("libpbc", advise)) {
////        free(search_path);
////		opal_output(0, "plm:yarn:ltdl_pbc: opal_lt_dlopenext for libpbc error: %s.\n", opal_lt_dlerror());
////		return -1;
////	}
//
////	if (!opal_lt_dlopenadvise("libmca_hdclient", advise)) {
//    if (!lt_dlopenadvise("libmca_hdclient", advise)) {
////		opal_output(0, "plm:yarn:ltdl_pbc: opal_lt_dlopenext for libmca_hdclient error: %s.\n", opal_lt_dlerror());
//    	opal_output(0, "plm:yarn:ltdl_pbc: opal_lt_dlopenext for libmca_hdclient error: %s.\n", lt_dlerror());
//		return -1;
//	}
//    opal_output(0, "plm:yarn:ltdl_pbc:  lt_dlopenadvise successful!!");
//
////    opal_lt_dladvise_destroy(&advise);
//    lt_dladvise_destroy(&advise);
//
//	return 0;
//}

static int plm_yarn_open(void)
{
	opal_output(0, "!!!!!!!!!!! in plm_yarn_open()");
//	int ret = ltdl_pbc();
//	if (0 != ret) {
//		opal_output(0, "plm:yarn:plm_yarn_open: init hdclient lib failed.\n");
//		return ORTE_ERROR;
//	}
    return ORTE_SUCCESS;
}

static int orte_plm_yarn_component_query(mca_base_module_t **module, int *priority)
{
	*priority = 0;

	OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
					"%s plm:yarn: available for selection",
					ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

	*module = (mca_base_module_t *) &orte_plm_yarn_module;
	return ORTE_SUCCESS;
}


static int plm_yarn_close(void)
{
    return ORTE_SUCCESS;
}

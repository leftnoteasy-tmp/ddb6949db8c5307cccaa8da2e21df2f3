/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Los Alamos National Security, LLC.  All rights
 *                         reserved. 
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

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <signal.h>
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#endif

#include "opal/mca/installdirs/installdirs.h"
#include "opal/util/argv.h"
#include "opal/util/output.h"
#include "opal/util/opal_environ.h"
#include "opal/util/path.h"
#include "opal/util/basename.h"
#include "opal/mca/base/mca_base_param.h"

#include "orte/constants.h"
#include "orte/types.h"
#include "orte/util/show_help.h"
#include "orte/util/name_fns.h"
#include "orte/util/regex.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_wait.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rmaps/rmaps.h"

#include "orte/orted/orted.h"

#include "orte/mca/plm/plm.h"
#include "orte/mca/plm/base/plm_private.h"
#include "plm_yarn.h"

#include "orte/mca/hdclient/hdclient.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/grpcomm/grpcomm.h"


extern char** environ;
int num_sync_daemons = 0;

/*
 * Local functions
 */
static int plm_yarn_init(void);
static int plm_yarn_launch_job(orte_job_t *jdata);
static int plm_yarn_terminate_orteds(void);
static int plm_yarn_signal_job(orte_jobid_t jobid, int32_t signal);
static int plm_yarn_finalize(void);


/*
 * Local intermediate functions
 */

static int launch_daemons(orte_job_t* jdata);
static int setup_daemon_proc_env_and_argv(orte_proc_t* proc, char **argv,
        int *argc, char **env);

static void yarn_hnp_sync_recv(int status, orte_process_name_t* sender,
                           opal_buffer_t* buffer, orte_rml_tag_t tag,
                           void* cbdata);

static int plm_yarn_actual_launch_procs(orte_job_t* jdata);
static int setup_proc_env_and_argv(orte_job_t* jdata, orte_app_context_t* app,
        orte_proc_t* proc, char **argv, char **env);

static void heartbeat_with_AM_cb(int fd, short event, void *data);


/*
 * Global variable
 */
orte_plm_base_module_1_0_0_t orte_plm_slurm_module = {
    plm_yarn_init,
    orte_plm_base_set_hnp_name,
    plm_yarn_launch_job,
    NULL,
    orte_plm_base_orted_terminate_job,
    plm_yarn_terminate_orteds,
    orte_plm_base_orted_kill_local_procs,
    plm_yarn_signal_job,
    plm_yarn_finalize
};

/*
 * Local variables
 */
static pid_t primary_srun_pid = 0;
static bool primary_pid_set = false;
static orte_jobid_t active_job = ORTE_JOBID_INVALID;
static bool launching_daemons;
static bool local_launch_available = false;

/**
* Init the module
 */
static int plm_yarn_init(void)
{
    int rc;

    if (ORTE_SUCCESS != (rc = orte_plm_base_comm_start())) {
        ORTE_ERROR_LOG(rc);
    }

    return rc;
}

/* setup argv for daemon process */
static int setup_daemon_proc_env_and_argv(orte_proc_t* proc, char **argv,
        int *argc, char **env)
{
    orte_job_t* daemons;
    int rc;
    char* param;

    /* get daemon job object */
    daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);

    /* prepend orted to argv */
    opal_argv_append(argc, &argv, "orted");

    /* ess */
    opal_argv_append(argc, &argv, "-mca");
    opal_argv_append(argc, &argv, "ess");
    opal_argv_append(argc, &argv, "env");

    /* jobid */
    opal_argv_append(argc, &argv, "-mca");
    opal_argv_append(argc, &argv, "orte_ess_jobid");
    if (ORTE_SUCCESS != (rc = orte_util_convert_jobid_to_string(&param, ORTE_PROC_MY_NAME->jobid))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    opal_argv_append(argc, &argv, param);
    free(param);

    /* vpid */
    opal_argv_append(argc, &argv, "-mca");
    opal_argv_append(argc, &argv, "orte_ess_vpid");
    if (ORTE_SUCCESS != (rc = orte_util_convert_vpid_to_string(&param, proc->name.vpid))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    opal_argv_append(argc, &argv, param);
    free(param);

    /* num processes */
    opal_argv_append(argc, &argv, "-mca");
    opal_argv_append(argc, &argv, "orte_ess_num_procs");
    asprintf(&param, "%lu", daemons->num_procs);
    opal_argv_append(argc, &argv, param);
    free(param);

    /* pass the uri of the hnp */
    asprintf(&param, "\\\"%s\\\"", orte_rml.get_contact_info());
    opal_argv_append(argc, &argv, "-mca");
    opal_argv_append(argc, &argv, "orte_hnp_uri");
    opal_argv_append(argc, &argv, param);
    free(param);

    /* oob */
    opal_argv_append(argc, &argv, "-mca");
    opal_argv_append(argc, &argv, "oob");
    opal_argv_append(argc, &argv, "tcp");

    /* odls */
    opal_argv_append(argc, &argv, "-mca");
    opal_argv_append(argc, &argv, "odls");
    opal_argv_append(argc, &argv, "yarn");

    /* add stdout, stderr to orted */
    opal_argv_append_nosize(&argv, "1><LOG_DIR>/stdout");
    opal_argv_append_nosize(&argv, "2><LOG_DIR>/stderr");

    /* print launch commandline and env when this env is specified */
    if (getenv("HAMSTER_VERBOSE")) {
        char* join_argv = opal_argv_join(argv, ' ');
        char* join_env = opal_argv_join(env, ' ');
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output, "%s plm:yarn launch_daemon argv=%s",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), join_argv));
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output, "%s plm:yarn launch_daemon env=%s",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), join_env));
        if (join_argv) {
            free(join_argv);
        }
        if (join_env) {
            free(join_env);
        }
    }
    return 0;
}


static int launch_daemons(orte_job_t* jdata)
{
    int i, rc;
    orte_proc_t* proc = NULL;
    char **argv;
    int argc;
    char **env;
    bool error_flag = false;

    orte_job_t* daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);

    /* 1. create launch message */
    /*
     message LaunchRequestProto {
         repeated LaunchContextProto launch_contexts = 1;
     }

     message LaunchContextProto {
         repeated string envars = 1;
         optional string args = 2;
         optional string host_name = 3;
         optional ProcessNameProto name = 4;
     }

     message ProcessNameProto {
         optional int32 jobid = 1;
         optional int32 vpid = 2;
     }
     */
    struct pbc_wmessage* request_msg = pbc_wmessage_new(orte_hdclient_pb_env, "LaunchRequestProto");
    if (!request_msg) {
        opal_output(0, "%s plm:yarn: failed to create AllocateRequestProto",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    env = opal_argv_copy(orte_launch_environ);
    /* start from 1 because we don't need launch HNP process */
    for (i = 1; i < daemons->num_procs; i++) {
        argv = NULL;
        argc = 0;
        /* setup env/argv  */
        proc = opal_pointer_array_get_item(daemons->procs, i);
        if (!proc) {
            opal_output(0, "%s plm:yarn:launch_daemons: daemons[%d] is NULL",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i);
            ORTE_ERROR_LOG(ORTE_ERROR_DEFAULT_EXIT_CODE);
        }

        if (0 != setup_daemon_proc_env_and_argv(proc, argv, &argc, env)) {
            opal_output(0,
                    "%s plm:yarn:launch_daemons: failed to setup env/argv of daemon proc[%d]",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i);
            ORTE_ERROR_LOG(ORTE_ERROR_DEFAULT_EXIT_CODE);
            error_flag = true;
            goto cleanup;
        }

        /* now start packing request_msg */
        struct pbc_wmessage *launch_contexts_msg = pbc_wmessage_message(request_msg, "launch_contexts");
        if (!launch_contexts_msg) {
            opal_output(0, "%s plm:yarn:launch_daemons create launch_contexts_msg failed",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            error_flag = true;
            goto cleanup;
        }

        while (*env) {
            pbc_wmessage_string(launch_contexts_msg, "envars", *env, strlen(*env));
            env++;
        }

        char* join_argv = opal_argv_join(argv, ' ');
        pbc_wmessage_string(launch_contexts_msg, "args", join_argv, strlen(join_argv));

        pbc_wmessage_string(launch_contexts_msg, "host_name", proc->node->name, strlen(proc->node->name));

        struct pbc_wmessage *proccess_name_msg = pbc_wmessage_message(
                launch_contexts_msg, "name");
        if (!proccess_name_msg) {
            opal_output(0, "%s plm:yarn:launch_daemons create proccess_name_msg failed",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            error_flag = true;
            goto cleanup;
        }

        rc = pbc_wmessage_integer(proccess_name_msg, "jobid", proc->name.jobid, 0);
        if (0 != rc) {
            opal_output(0,
                    "%s plm:yarn:launch_daemons pack jobid in proccess_name_msg failed",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            error_flag = true;
            goto cleanup;
        }

        rc = pbc_wmessage_integer(proccess_name_msg, "vpid", proc->name.vpid, 0);
        if (0 != rc) {
            opal_output(0, "%s plm:yarn:launch_daemons pack vpid in proccess_name_msg failed",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            error_flag = true;
            goto cleanup;
        }

cleanup:
        /* free argv and env for this proc */
        if (argv) {
            opal_argv_free(argv);
        }
        if (env) {
            opal_argv_free(env);
        }
        if (error_flag) {
            return ORTE_ERROR;
        }
    }

    /* 2. send launch deamon procs request msg */
    rc = orte_hdclient_send_message_and_delete(request_msg, HAMSTER_MSG_LAUNCH);
    if (rc != 0) {
        opal_output(0, "%s plm:yarn:launch_daemons error happened when send launch proc request to AM",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    /* 3. recv response and parse the msg*/
    /*
     message LaunchResponseProto {
         repeated LaunchResultProto results = 1;
     }

     message LaunchResultProto {
         optional ProcessNameProto name = 1;
         optional bool success = 2;
     }

     message ProcessNameProto {
         optional int32 jobid = 1;
         optional int32 vpid = 2;
     }
     */
    struct pbc_rmessage* response_msg = orte_hdclient_recv_message("LaunchResponseProto");
    if (!response_msg) {
        opal_output(0,
                "%s plm:yarn:launch_daemons error happened when recv launch response msg from AM",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    int n = pbc_rmessage_size(response_msg, "results");
    if (n <= 0) {
        opal_output(0, "%s plm:yarn:launch_daemons got n(=%d) <= 0, please check",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), n);
        pbc_rmessage_delete(response_msg);
        return ORTE_ERROR;
    }

    for (i = 0; i < n; i++) {
        struct pbc_rmessage* results_msg = pbc_rmessage_message(response_msg, "results", i);
        if (!results_msg) {
            opal_output(0,
                    "%s plm:yarn:launch_daemons: error when parse returned launch results from AM",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            pbc_rmessage_delete(response_msg);
            return ORTE_ERROR;
        }

        struct pbc_rmessage* proc_name_msg = pbc_rmessage_message(results_msg, "name", 0); //?
        if (!proc_name_msg) {
            opal_output(0,
                    "%s plm:yarn:launch_daemons: error when parse returned proc_name_msg from AM",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            pbc_rmessage_delete(response_msg);
            return ORTE_ERROR;
        }

        orte_jobid_t jobid =  pbc_rmessage_integer(proc_name_msg, "jobid", 0, NULL);
        orte_vpid_t vpid = pbc_rmessage_integer(proc_name_msg, "vpid", 0, NULL);

        bool success = pbc_rmessage_integer(results_msg, "success", 0, NULL);
        if (!success) {
            daemons->state = ORTE_JOB_STATE_FAILED_TO_START;
            pbc_rmessage_delete(response_msg);
            opal_output(0,
                    "%s plm:yarn:launch_daemons: launch deamon proc failed when jobid = %u, vpid = %u",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), jobid, vpid);
            return ORTE_ERROR;
        }
    }

    /* 4. delete response_msg */
    pbc_rmessage_delete(response_msg);

    OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                    "%s plm:yarnlaunch_daemons: launch daemon proc successfully with AM",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    return ORTE_SUCCESS;
}

static void heartbeat_with_AM_cb(int fd, short event, void *data)
{
    orte_job_t *jdata = (orte_job_t*)data;

//    active_job_completed_callback = false;
//    orte_plm_base_check_job_completed(jdata);



    /* next heartbeat */
    opal_event_t *ev = NULL;
    ev = (opal_event_t*) malloc(sizeof(opal_event_t));

    struct timeval delay;
    delay.tv_sec = 1;
    delay.tv_usec = 0;
    opal_evtimer_add(ev, &delay);

    opal_evtimer_set(ev, heartbeat_with_AM_cb, jdata);
}

/* When working in this function, ALWAYS jump to "cleanup" if
 * you encounter an error so that orterun will be woken up and
 * the job can cleanly terminate
 */
static int plm_yarn_launch_job(orte_job_t *jdata)
{
    orte_app_context_t **apps;
    orte_node_t **nodes;
    orte_std_cntr_t n;
    orte_job_map_t *map;
    char *jobid_string = NULL;
    char *param;
    char **argv = NULL;
    int argc;
    int rc;
    char *tmp;
    char** env = NULL;
    char* var;
    char *nodelist_flat;
    char **nodelist_argv;
    char *name_string;
    char **custom_strings;
    int num_args, i;
    char *cur_prefix;
    struct timeval launchstart, launchstop;
    int proc_vpid_index;
    orte_jobid_t failed_job;
    bool failed_launch=true;
    bool using_regexp=false;

    orte_proc_t* proc = NULL;

    //============heartbeat with AM======
    opal_event_t *ev = NULL;
    ev = (opal_event_t*) malloc(sizeof(opal_event_t));

    struct timeval delay;
    delay.tv_sec = 1;
    delay.tv_usec = 0;
    opal_evtimer_add(ev, &delay);

    opal_evtimer_set(ev, heartbeat_with_AM_cb, jdata);
    //===================================

    if (NULL == jdata) {
        /* just launching debugger daemons */
        active_job = ORTE_JOBID_INVALID;
        goto launch_apps;
    }

    if (jdata->controls & ORTE_JOB_CONTROL_DEBUGGER_DAEMON) {
        /* debugger daemons */
        active_job = jdata->jobid;
        goto launch_apps;
    }

    if (jdata->controls & ORTE_JOB_CONTROL_LOCAL_SLAVE) {
        /* if this is a request to launch a local slave,
         * then we will not be launching an orted - we will
         * directly ssh the slave process itself. No mapping
         * is performed to support this - the caller must
         * provide all the info required to launch the job,
         * including the target hosts
         */
        if (!local_launch_available) {
            /* if we can't support this, then abort */
            orte_show_help("help-plm-yarn.txt", "no-local-slave-support", true);
            return ORTE_ERR_FAILED_TO_START;
        }
        return orte_plm_base_local_slave_launch(jdata);
    }
    
    /* if we are timing, record the start time */
    if (orte_timing) {
        gettimeofday(&orte_plm_globals.daemonlaunchstart, NULL);
    }
    
    /* flag the daemons as failing by default */
    failed_job = ORTE_PROC_MY_NAME->jobid;
    
    if (orte_timing) {
        if (0 != gettimeofday(&launchstart, NULL)) {
            opal_output(0, "plm_yarn: could not obtain job start time");
            launchstart.tv_sec = 0;
            launchstart.tv_usec = 0;
        }        
    }
    
    /* indicate the state of the launch */
    launching_daemons = true;
    
    /* setup the job */
    if (ORTE_SUCCESS != (rc = orte_plm_base_setup_job(jdata))) {
        ORTE_ERROR_LOG(rc);
        goto cleanup;
    }

    OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                         "%s plm:yarn: launching job %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));
    
    /* set the active jobid */
     active_job = jdata->jobid;
    
    /* Get the map for this job */
    if (NULL == (map = orte_rmaps.get_job_map(active_job))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        rc = ORTE_ERR_NOT_FOUND;
        goto cleanup;
    }
    apps = (orte_app_context_t**)jdata->apps->addr;
    nodes = (orte_node_t**)map->nodes->addr;
        
    if (0 == map->num_new_daemons) {
        /* no new daemons required - just launch apps */
        OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                             "%s plm:yarn: no new daemons to launch",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        goto launch_apps;
    }

    /* launch daemons */
    if (ORTE_SUCCESS != (rc = launch_daemons(jdata))) {
        opal_output(0, "%s plm:yarn:plm_yarn_launch_job: launch deamon failed",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        goto cleanup;
    }
    
    /* wait for daemons to callback */
    if (ORTE_SUCCESS
            != (rc = orte_plm_base_daemon_callback(map->num_new_daemons))) {
        OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                        "%s plm:yarn:plm_yarn_launch_job: daemon launch failed for job %s on error %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_JOBID_PRINT(active_job), ORTE_ERROR_NAME(rc)));
        goto cleanup;
    }

launch_apps:
    /* get here if daemons launch okay - any failures now by apps */
    launching_daemons = false;
    failed_job = active_job;

    /* here, we make a sync between each orted and hnp */
    /* register recv callback for daemons sync request */
    if (ORTE_SUCCESS != (rc = orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                    ORTE_RML_TAG_ORTED_CALLBACK, ORTE_RML_PERSISTENT,   // ORTE_RML_TAG_ORTED_CALLBACK
                    yarn_hnp_sync_recv, jdata))) {
        ORTE_ERROR_LOG(rc);
        goto cleanup;
    }

    if (ORTE_SUCCESS != (rc = orte_plm_base_launch_apps(active_job))) {
        OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                        "%s plm:slurm:plm_yarn_launch_job: launch of apps failed for job %s on error %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        ORTE_JOBID_PRINT(active_job), ORTE_ERROR_NAME(rc)));
        goto cleanup;
    }

    /* declare the launch a success */
    failed_launch = false;
    
    if (orte_timing) {
        if (0 != gettimeofday(&launchstop, NULL)) {
            opal_output(0,
                    "%s plm:yarn:plm_yarn_launch_job: could not obtain stop time",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        } else {
            opal_output(0,
                    "%s plm:yarn:plm_yarn_launch_job: total job launch time is %ld usec",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    (launchstop.tv_sec - launchstart.tv_sec) * 1000000
                            + (launchstop.tv_usec - launchstart.tv_usec));
        }
    }

    if (ORTE_SUCCESS != rc) {
        opal_output(0,
                "%s plm:yarn:plm_yarn_launch_job: start_procs returned error %d",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), rc);
        goto cleanup;
    }

cleanup:
    /* check for failed launch - if so, force terminate */
    if (failed_launch) {
        orte_plm_base_launch_failed(failed_job, -1, ORTE_ERROR_DEFAULT_EXIT_CODE, ORTE_JOB_STATE_FAILED_TO_START);
    }
    
    return rc;
}

static void yarn_hnp_sync_recv(int status, orte_process_name_t* sender,
                           opal_buffer_t* buffer, orte_rml_tag_t tag,
                           void* cbdata)
{
    /* get daemon job object */
    orte_job_t* daemons = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    /* get user's job object */
    orte_job_t* jdata = (orte_job_t*)cbdata;
    opal_buffer_t *msg;
    int rc;

    num_sync_daemons++;

    /* we got all daemons synced */
    if (daemons->num_procs == num_sync_daemons) {
        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                        "%s plm:yarn:yarn_hnp_sync_recv: we got all daemons sync, will launch proc in NM",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

        msg = OBJ_NEW(opal_buffer_t);
        if (ORTE_SUCCESS != (rc = orte_grpcomm.xcast(ORTE_PROC_MY_NAME->jobid, msg, ORTE_RML_TAG_DAEMON))) { //ORTE_RML_TAG_DAEMON
            ORTE_ERROR_LOG(rc);
            opal_output(0, "%s plm:yarn:yarn_hnp_sync_recv: failed to send sync response to daemon processes.",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            OBJ_RELEASE(msg);
        }
        OBJ_RELEASE(msg);

        /* here, we actually launch procs */
        rc = plm_yarn_actual_launch_procs(jdata);

        if (ORTE_SUCCESS != rc) {
            orte_plm_base_launch_failed(jdata->jobid, -1, ORTE_ERROR_DEFAULT_EXIT_CODE, ORTE_JOB_STATE_FAILED_TO_START);
        }
    }

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                "%s plm:yarn:yarn_hnp_sync_recv: we got [%d/%d] daemons yarn sync request",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), num_sync_daemons, daemons->num_procs));
}




static int plm_yarn_actual_launch_procs(orte_job_t* jdata)
{
    int rc;
    orte_vpid_t idx, i;
    orte_app_context_t* app = NULL;
    orte_proc_t *proc = NULL;

    char **argv;
    char **env;
    bool success = false;
    bool error_flag = false;

    OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                    "%s plm:yarn:launch_apps for job %s",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    ORTE_JOBID_PRINT(jdata->jobid)));

    /* 1. create launch message */
    /*
     message LaunchRequestProto {
     repeated LaunchContextProto launch_contexts = 1;
     }

     message LaunchContextProto {
     repeated string envars = 1;
     optional string args = 2;
     optional string host_name = 3;
     optional ProcessNameProto name = 4;
     }

     message ProcessNameProto {
     optional int32 jobid = 1;
     optional int32 vpid = 2;
     }
     */
    struct pbc_wmessage* request_msg = pbc_wmessage_new(orte_hdclient_pb_env, "LaunchRequestProto");
    if (!request_msg) {
        opal_output(0, "%s plm:yarn: failed to create AllocateRequestProto",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }


    for (idx = 0; idx < jdata->num_procs; idx++) {
        argv = NULL;
        env = NULL;

        /* get proc and app */
        proc = (orte_proc_t*) opal_pointer_array_get_item(jdata->procs, idx);
        app = (orte_app_context_t*) opal_pointer_array_get_item(jdata->apps,
                proc->app_idx);

        rc = setup_proc_env_and_argv(jdata, app, proc, argv, env);
        if (rc != 0) {
            opal_output(0,
                    "%s plm:yarn:plm_yarn_actual_launch_procs: setup_proc_env_and_argv failed.",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            ORTE_ERROR_LOG(rc);
            jdata->state = ORTE_JOB_NEVER_LAUNCHED;
            error_flag = true;
            goto cleanup;
        }

        /* print launch commandline and env when this env is specified */
        if (getenv("HAMSTER_VERBOSE")) {
            char* join_argv = opal_argv_join(argv, ' ');
            char* join_env = opal_argv_join(env, ' ');
            OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output, "%s plm:yarn launch argv=%s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), join_argv));OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output, "%s plm:yarn launch env=%s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), join_env));
            if (join_argv) {
                free(join_argv);
            }
            if (join_env) {
                free(join_env);
            }
        }

        OPAL_OUTPUT_VERBOSE((5, orte_plm_base_framework.framework_output,
                        "%s plm:yarn:plm_yarn_actual_launch_procs: after setup env and argv for proc=%d.",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), idx));

        /* now start packing request_msg */
        struct pbc_wmessage *launch_contexts_msg = pbc_wmessage_message(request_msg, "launch_contexts");
        if (!launch_contexts_msg) {
            opal_output(0,
                    "%s plm:yarn:plm_yarn_actual_launch_procs: create launch_contexts_msg failed",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            error_flag = true;
            goto cleanup;
        }

        while (*env) {
            pbc_wmessage_string(launch_contexts_msg, "envars", *env, strlen(*env));
            env++;
        }

        char* join_argv = opal_argv_join(argv, ' ');
        pbc_wmessage_string(launch_contexts_msg, "args", join_argv, strlen(join_argv));

        pbc_wmessage_string(launch_contexts_msg, "host_name", proc->node->name, strlen(proc->node->name));

        struct pbc_wmessage *proccess_name_msg = pbc_wmessage_message(launch_contexts_msg, "name");
        if (!proccess_name_msg) {
            opal_output(0,
                    "%s plm:yarn:plm_yarn_actual_launch_procs: create proccess_name_msg failed",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            error_flag = true;
            goto cleanup;
        }

        rc = pbc_wmessage_integer(proccess_name_msg, "jobid", proc->name.jobid, 0);
        if (0 != rc) {
            opal_output(0,
                    "%s plm:yarn:plm_yarn_actual_launch_procs: pack jobid in proccess_name_msg failed",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            error_flag = true;
            goto cleanup;
        }

        rc = pbc_wmessage_integer(proccess_name_msg, "vpid", proc->name.vpid, 0);
        if (0 != rc) {
            opal_output(0,
                    "%s plm:yarn:plm_yarn_actual_launch_procs: pack vpid in proccess_name_msg failed",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            error_flag = true;
            goto cleanup;
        }

cleanup:
        /* free argv and env for this proc */
        if (argv) {
            opal_argv_free(argv);
        }
        if (env) {
            opal_argv_free(env);
        }
        if (error_flag) {
            return ORTE_ERROR;
        }
    }

    /* 2. send launch procs request msg */
    rc = orte_hdclient_send_message_and_delete(request_msg, HAMSTER_MSG_LAUNCH);
    if (rc != 0) {
        opal_output(0,
                "%s plm:yarn:plm_yarn_actual_launch_procs: error happened when send launch proc request to AM",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    /* 3. recv response and parse the msg*/
    /*
     message LaunchResponseProto {
     repeated LaunchResultProto results = 1;
     }

     message LaunchResultProto {
     optional ProcessNameProto name = 1;
     optional bool success = 2;
     }

     message ProcessNameProto {
     optional int32 jobid = 1;
     optional int32 vpid = 2;
     }
     */
    struct pbc_rmessage* response_msg = orte_hdclient_recv_message("LaunchResponseProto");
    if (!response_msg) {
        opal_output(0,
                "%s plm:yarn:plm_yarn_actual_launch_procs: error happened when recv launch response msg from AM",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    int n = pbc_rmessage_size(response_msg, "results");
    if (n <= 0) {
        opal_output(0,
                "%s plm:yarn:plm_yarn_actual_launch_procs: got n(=%d) <= 0, please check",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), n);
        return ORTE_ERROR;
    }

    for (i = 0; i < n; i++) {
        struct pbc_rmessage* results_msg = pbc_rmessage_message(response_msg, "results", i);
        if (!results_msg) {
            opal_output(0,
                    "%s plm:yarn:plm_yarn_actual_launch_procs: error when parse returned launch results from AM",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            return ORTE_ERROR;
        }

        struct pbc_rmessage* proc_name_msg = pbc_rmessage_message(results_msg, "name", 0); //?
        if (!proc_name_msg) {
            opal_output(0,
                    "%s plm:yarn:plm_yarn_actual_launch_procs: error when parse returned proc_name_msg from AM",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            return ORTE_ERROR;
        }

        orte_jobid_t jobid = pbc_rmessage_integer(proc_name_msg, "jobid", 0, NULL);
        orte_vpid_t vpid = pbc_rmessage_integer(proc_name_msg, "vpid", 0, NULL);

        success = pbc_rmessage_integer(results_msg, "success", 0, NULL);
        if (!success) {
            jdata->state = ORTE_JOB_STATE_FAILED_TO_START;
            opal_output(0,
                    "%s plm:yarn:launch_daemons: launch deamon proc failed when jobid = %u, vpid = %u",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), jobid, vpid);
            pbc_rmessage_delete(response_msg);
            return ORTE_ERROR;
        }
    }

    /* 4. delete response_msg */
    jdata->state = ORTE_JOB_STATE_RUNNING;

    OPAL_OUTPUT_VERBOSE((5, orte_ras_base_framework.framework_output,
                        "%s plm:yarn:plm_yarn_actual_launch_procs: launch procs successfully via AM",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    pbc_rmessage_delete(response_msg);
    return ORTE_SUCCESS;
}

/*
 * setup env and argv for specified process
 */
static int setup_proc_env_and_argv(orte_job_t* jdata, orte_app_context_t* app,
        orte_proc_t* proc, char **argv, char **env)
{
    char* param;
    char* value;
    char* vp_id_str;
    char* job_id_str;
    int rc;
    int i, num_nodes;

    /* obtain app->argv */
    if (!(app->argv)) {
        opal_output(0, "%s plm::yarn::setup_proc_env_and_argv: app->argv is null",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return ORTE_ERROR;
    }

    argv = opal_argv_copy(app->argv);

    if (ORTE_SUCCESS != orte_util_convert_jobid_to_string(&job_id_str, jdata->jobid)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    if (ORTE_SUCCESS != orte_util_convert_vpid_to_string(&vp_id_str, proc->name.vpid)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    // add stdout, stderr to app
    opal_argv_append_nosize(&argv, "1><LOG_DIR>/stdout");
    opal_argv_append_nosize(&argv, "2><LOG_DIR>/stderr");

    // add java executor to app
    opal_argv_prepend_nosize(&argv, vp_id_str);
    opal_argv_prepend_nosize(&argv, job_id_str);
    opal_argv_prepend_nosize(&argv, "com.greenplum.hamster.yarnexecutor.YarnExecutor");
    opal_argv_prepend_nosize(&argv, "hamster-cli.jar");
    opal_argv_prepend_nosize(&argv, "-cp");
    opal_argv_prepend_nosize(&argv, getenv("HAMSTER_JAVA_OPT")==NULL ? "-Xmx32M -Xms8M" : getenv("HAMSTER_JAVA_OPT"));
    opal_argv_prepend_nosize(&argv, "$JAVA_HOME/bin/java");

    int tmp_idx = 0;
    while (argv[tmp_idx]) {
        opal_output(0, "%s  plm::yarn::setup_proc_env_and_argv: argv %d:%s",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp_idx, argv[tmp_idx]);
        tmp_idx++;
    }

    /* obtain app->env */
    env = opal_environ_merge(environ, app->env);

    if (!proc->node) {
        opal_output(0, "%s plm::yarn::setup_proc_env_and_argv: node of proc[%d] is NULL",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), proc->name.vpid);
        return ORTE_ERROR;
    }

    if (!proc->node->daemon) {
        opal_output(0, "%s plm::yarn::setup_proc_env_and_argv: daemon of node[%s] is NULL",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), proc->node->name);
        return ORTE_ERROR;
    }

    // pass the daemon's name
    if (OPAL_SUCCESS
            != mca_base_var_env_name("orte_local_daemon_uri", &param)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        rc = ORTE_ERR_OUT_OF_RESOURCE;
        return rc;
    }
    opal_setenv(param, proc->node->daemon->rml_uri, true, &env);
    free(param);

    /* pass my contact info */
    if (OPAL_SUCCESS != mca_base_var_env_name("orte_hnp_uri", &param)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        rc = ORTE_ERR_OUT_OF_RESOURCE;
        return rc;
    }
    opal_setenv(param, orte_process_info.my_hnp_uri, true, &env);
    free(param);

    /* pass the jobid */
    if (OPAL_SUCCESS != mca_base_var_env_name("orte_ess_jobid", &param)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        rc = ORTE_ERR_OUT_OF_RESOURCE;
        return rc;
    }
    opal_setenv(param, job_id_str, true, &env);
    free(param);
    free(job_id_str);

    /* pass the rank */
    if (OPAL_SUCCESS != mca_base_var_env_name("orte_ess_vpid", &param)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        rc = ORTE_ERR_OUT_OF_RESOURCE;
        return rc;
    }
    opal_setenv(param, vp_id_str, true, &env);
    free(param);
    opal_setenv("OMPI_COMM_WORLD_RANK", vp_id_str, true, &env);
    free(vp_id_str);  /* done with this now */

    /* pass local rank */
    asprintf(&value, "%lu", (unsigned long) proc->local_rank);
    opal_setenv("OMPI_COMM_WORLD_LOCAL_RANK", value, true, &env);
    free(value);

    /* pass node rank */
    asprintf(&value, "%lu", (unsigned long) proc->node_rank);
    opal_setenv("OMPI_COMM_WORLD_NODE_RANK", value, true, &env);

    /* set an mca param for it too */
    if (OPAL_SUCCESS != mca_base_var_env_name("orte_ess_node_rank", &param)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        rc = ORTE_ERR_OUT_OF_RESOURCE;
        return rc;
    }
    opal_setenv(param, value, true, &env);
    free(param);
    free(value);

    /* pass the number of nodes involved in this job */
    if (OPAL_SUCCESS != mca_base_var_env_name("orte_num_nodes", &param)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        rc = ORTE_ERR_OUT_OF_RESOURCE;
        return rc;
    }
    /* we have to count the number of nodes as the size of orte_node_pool
     * is only guaranteed to be equal or larger than that number - i.e.,
     * the pointer_array increases the size by a block each time, so some
     * of the locations are left empty
     */
    num_nodes = 0;
    for (i = 0; i < orte_node_pool->size; i++) {
        if (NULL != opal_pointer_array_get_item(orte_node_pool, i)) {
            num_nodes++;
        }
    }
    asprintf(&value, "%d", num_nodes);
    opal_setenv(param, value, true, &env);
    free(param);
    free(value);

    /* setup yield schedule */
    if (OPAL_SUCCESS != mca_base_var_env_name("mpi_yield_when", &param)) {
        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
        rc = ORTE_ERR_OUT_OF_RESOURCE;
        return rc;
    }
    opal_setenv(param, "0", false, &env);
    free(param);

    /* set MPI universe envar */
    rc = orte_ess_env_put(jdata->num_procs, proc->node->num_procs, &env);
    asprintf(&value, "%ld", (long) jdata->num_procs);
    opal_setenv("OMPI_UNIVERSE_SIZE", value, true, &env);
    free(value);

//    /* pass collective ids for the std MPI operations */
//    if (OPAL_SUCCESS != mca_base_var_env_name("orte_peer_modex_id", &param)) {
//        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
//        rc = ORTE_ERR_OUT_OF_RESOURCE;
//        return rc;
//    }
//    asprintf(&value, "%d", jdata->peer_modex);
//    opal_setenv(param, value, true, &env);
//    free(param);
//    free(value);
//
//    if (OPAL_SUCCESS
//            != mca_base_var_env_name("orte_peer_init_barrier_id", &param)) {
//        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
//        rc = ORTE_ERR_OUT_OF_RESOURCE;
//        return rc;
//    }
//    asprintf(&value, "%d", jdata->peer_init_barrier);
//    opal_setenv(param, value, true, &env);
//    free(param);
//    free(value);
//
//    if (OPAL_SUCCESS
//            != mca_base_var_env_name("orte_peer_fini_barrier_id", &param)) {
//        ORTE_ERROR_LOG(ORTE_ERR_OUT_OF_RESOURCE);
//        rc = ORTE_ERR_OUT_OF_RESOURCE;
//        return rc;
//    }
//    asprintf(&value, "%d", jdata->peer_fini_barrier);
//    opal_setenv(param, value, true, &env);

    /* finally, we will set/unset some mca param to select modules */
    opal_unsetenv("OMPI_MCA_plm", &env);
    opal_unsetenv("OMPI_MCA_ras", &env);
    opal_unsetenv("OMPI_MCA_ess", &env);
    opal_unsetenv("OMPI_MCA_errmgr", &env);
    return 0;
}

/**
* Terminate the orteds for a given job
 */
static int plm_yarn_terminate_orteds(void)
{
    int rc;
    orte_job_t *jdata;
    
    /* tell them to die without sending a reply - we will rely on the
     * waitpid to tell us when they have exited!
     */
    if (ORTE_SUCCESS != (rc = orte_plm_base_orted_exit(ORTE_DAEMON_EXIT_CMD))) {
        ORTE_ERROR_LOG(rc);
    }
    
    /* check to see if the primary pid is set. If not, this indicates
     * that we never launched any additional daemons, so we cannot
     * not wait for a waitpid to fire and tell us it's okay to
     * exit. Instead, we simply trigger an exit for ourselves
     */
    if (!primary_pid_set) {
        OPAL_OUTPUT_VERBOSE((1, orte_plm_globals.output,
                             "%s plm:slurm: primary daemons complete!",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        jdata = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
        jdata->state = ORTE_JOB_STATE_TERMINATED;
        /* need to set the #terminated value to avoid an incorrect error msg */
        jdata->num_terminated = jdata->num_procs;
        orte_trigger_event(&orteds_exit);
    }
    
    return rc;
}

/**
 * Signal all the processes in the child srun by sending the signal directly to it
 */
static int plm_yarn_signal_job(orte_jobid_t jobid, int32_t signal)
{
    return ORTE_SUCCESS;
}


static int plm_yarn_finalize(void)
{
    int rc;
    
    /* cleanup any pending recvs */
    if (ORTE_SUCCESS != (rc = orte_plm_base_comm_stop())) {
        ORTE_ERROR_LOG(rc);
    }
    
    return ORTE_SUCCESS;
}
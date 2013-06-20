package com.pivotal.hamster.common;

public interface HamsterConfig {
  public final String HAMSTER_HOME_ENV_KEY = "HAMSTER_CORE_HOME";
  public final String APP_ID_ENV_KEY = "APP_ID_ENV";
  public final String CLUSTER_TIMESTAMP_ENV_KEY = "CLUSTER_TIMESTAMP_ENV";
  public final String TALKBACK_HOST = "TALKBACK_HOST";
  public final String TALKBACK_PORT = "TALKBACK_PORT";
  
  public final String PB_DIR_ENV_KEY = "YARN_PB_DIR";
  public final String YARN_VERSION_ENV_KEY = "YARN_VERSION";
  
  public final String PID_ROOT_DIR_PROPERTY_KEY = "com.greenplum.hamster.pid.root";
  public final String DEFAULT_PID_ROOT_DIR = "/tmp/hamster-pid";
  
  /* user can add some envs that help debug (like make a high verbose level, etc.) 
   * split by ";", e.g ENV1=xx;ENV2=yy; (no additional space)
   */
  public final String PROPERTY_DEBUG_ENVS_KEY = "com.greenplum.hamster.debug.envs";
  
  /* RM host and port for AM */
  public final String YARN_RM_SCHEDULER_HOSTNAME_ENV_KEY = "YARN_RM_SCHEDULER_HOSTNAME";
  public final String YARN_RM_SCHEDULER_PORT_ENV_KEY = "YARN_RM_SCHEDULER_PORT";
  
  /* Hamster Properties */
  /* Home of hamster */
  public final String OMPI_HOME_PROPERTY_KEY = "com.greenplum.ompi.home";
  public final String OMPI_LOCAL_TARBALL_PATH_KEY = "com.greenplum.ompi.tarball.path";
  
  /* Do we pre-installed hamster in nodes in YARN cluster? 
   * by default, we will consider all nodes have hamster installed in hamster.home
   */
  public final String OMPI_PREINSTALL_PROPERTY_KEY = "com.greenplum.ompi.preinstall"; 
  public final boolean OMPI_PREINSTALL_DEFAULT_VALUE = false;
    
  public final String DEFAULT_LOCALRESOURCE_SERIALIZED_FILENAME = "hamster_localresource_pb";
  public final String HAMSTER_ENABLED_LOGKEYS_KEY = "com.greenplum.hamster.enabled.logkeys";
  
  /**
   * default un-tar dir in NM for AM & launched processes when user specified *not* pre-install
   * YARN will un-tar hamster binaries to this directory
   */
  public final String DEFAULT_OMPI_INSTALL_DIR = "hamster-ompi";
  public final String DEFAULT_HAMSTER_INSTALL_DIR = "hamster-core";
  
  public final String YARN_PB_FILE_ENV_KEY = "YARN_PB_FILE";
  
  /**
   * how many times that user will wait for log aggregation finished (in ms)
   */
  public final String HAMSTER_LOG_AGGREGATION_WAIT_TIME = "com.greenplum.hamster.log.waittime";
  public final int DEFAULT_HAMSTER_LOG_WAIT_TIME = 5000;
      
  public static final String HAMSTER_CONFIG_PREFIX = "com.pivotal.hamster.";
  
  /* hnp expire time */
  public static final String HAMSTER_HNP_LIVENESS_EXPIRE_TIME = HAMSTER_CONFIG_PREFIX + "hnp.liveness.expiry-internal-ms";
  public static final int DEFAULT_HAMSTER_HNP_LIVENESS_EXPIRE_TIME = 60 * 1000 * 10; // 10 min 
  
  /* hnp pull interval */
  public static final String HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME = HAMSTER_CONFIG_PREFIX + "pull.rm-interval-ms";
  public static final int DEFAULT_HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME = 5000; // 5 sec
  
  /* file name for serialized pb file */
  public static final String HAMSTER_PB_FILE = "hamster_localresource_pb";
  
  /* umbilical port for HNP connect AM */
  public static final String AM_UMBILICAL_PORT_ENV_KEY = "AM_UMBILICAL_PORT";
  
  /* default value of how many proc in one node when we don't have maximum resource */
  public static final int DEFAULT_N_PROC_IN_ONE_NODE = 16;
}

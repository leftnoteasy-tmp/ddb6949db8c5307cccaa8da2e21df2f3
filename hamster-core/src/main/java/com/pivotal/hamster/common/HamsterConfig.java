package com.pivotal.hamster.common;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

public interface HamsterConfig {
  public static final String HAMSTER_CONFIG_PREFIX = "com.pivotal.hamster.";
  
  /* Hamster Properties */
  /* Home of hamster */
  public final String OMPI_HOME_PROPERTY_KEY = HAMSTER_CONFIG_PREFIX + "ompi.home";
  
  /*
   * configuration names for allocation strategy
   */
  public static final String ALLOCATION_STRATEGY_KEY = HAMSTER_CONFIG_PREFIX + "allocation.strategy.name";
  public static final String PROBABILITY_BASED_ALLOCATION_STRATEGY = "probability-based";
  public static final String NAIVE_ALLOCATION_STRATEGY = "naive";
  public static final String DEFAULT_HAMSTER_ALLOCATION_STRATEGY = NAIVE_ALLOCATION_STRATEGY;
  public static final String USER_POLICY_DRIVEN_ALLOCATION_STRATEGY = "user-driven";
  public static final String USER_POLICY_HOST_LIST_KEY = HAMSTER_CONFIG_PREFIX + "user.policy.hostlist";
  public static final String USER_POLICY_MPROC_KEY = HAMSTER_CONFIG_PREFIX + "user.policy.mproc";
  public static final String USER_POLICY_MNODE_KEY = HAMSTER_CONFIG_PREFIX + "user.policy.mnode";
  
  /*
   * configuration for username
   */
  public static final String USER_NAME_KEY = HAMSTER_CONFIG_PREFIX + "user.name";
    
  /* hnp expire time */
  public static final String HAMSTER_HNP_LIVENESS_EXPIRE_TIME = HAMSTER_CONFIG_PREFIX + "hnp.liveness.expiry-internal-ms";
  public static final int DEFAULT_HAMSTER_HNP_LIVENESS_EXPIRE_TIME = 60 * 1000 * 10; // 10 min 
  
  /* hnp pull interval */
  public static final String HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME = HAMSTER_CONFIG_PREFIX + "pull.rm-interval-ms";
  public static final int DEFAULT_HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME = 5000; // 5 sec
  
  public final String YARN_PB_FILE_ENV_KEY = "YARN_PB_FILE";
  
  /* file name for serialized pb file */  
  public static final String DEFAULT_LOCALRESOURCE_SERIALIZED_FILENAME = "hamster_localresource_pb";
  
  /* file name for serialized configuration file */
  public static final String DEFAULT_LOCALCONF_SERIALIZED_FILENAME = "hamster_localconf_serilized";
  
  /* umbilical port for HNP connect AM */
  public static final String AM_UMBILICAL_PORT_ENV_KEY = "AM_UMBILICAL_PORT";
  
  /* allocation time */
  public static final String ALLOCATION_TIMEOUT_KEY = HAMSTER_CONFIG_PREFIX
      + "allocation.timeout-ms";
  public static final int DEFAULT_ALLOCATION_TIMEOUT = 
      YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS;

  /* envs for resource specified when launching containers */
  public static final String HAMSTER_MEM_ENV_KEY = "HAMSTER_MEM";
  public static final String HAMSTER_CPU_ENV_KEY = "HAMSTER_CPU";
  public static final int DEFAULT_HAMSTER_MEM = 1024;
  public static final int DEFAULT_HAMSTER_CPU = 1;
}

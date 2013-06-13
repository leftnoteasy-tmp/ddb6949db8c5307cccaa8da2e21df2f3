package com.pivotal.hamster.appmaster;

public interface HamsterConfig {
  public static final String HAMSTER_CONFIG_PREFIX = "com.pivotal.hamster.";
  
  /* hnp expire time */
  public static final String HAMSTER_HNP_LIVENESS_EXPIRE_TIME = HAMSTER_CONFIG_PREFIX + "hnp.liveness.expiry-internal-ms";
  public static final int DEFAULT_HAMSTER_HNP_LIVENESS_EXPIRE_TIME = 60 * 1000 * 10; // 10 min 
  
  /* hnp pull interval */
  public static final String HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME = HAMSTER_CONFIG_PREFIX + "pull.rm-interval-ms";
  public static final int DEFAULT_HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME = 5000; // 5 sec
  
  /* file name for serialized pb file */
  public static final String HAMSTER_PB_FILE = "hamster_localresource_pb";
  
  /* default value of how many proc in one node when we don't have maximum resource */
  public static final int DEFAULT_N_PROC_IN_ONE_NODE = 16;
}

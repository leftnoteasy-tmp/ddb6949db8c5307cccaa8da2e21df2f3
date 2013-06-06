package com.pivotal.hamster.appmaster;

public interface HamsterConfig {
  public static final String HAMSTER_CONFIG_PREFIX = "com.pivotal.hamster.";
  
  /* hnp expire time */
  public static final String HAMSTER_HNP_LIVENESS_EXPIRE_TIME = HAMSTER_CONFIG_PREFIX + "hnp.liveness.expiry-internal-ms";
  public static final int DEFAULT_HAMSTER_HNP_LIVENESS_EXPIRE_TIME = 60 * 1000; // 1 min 
}

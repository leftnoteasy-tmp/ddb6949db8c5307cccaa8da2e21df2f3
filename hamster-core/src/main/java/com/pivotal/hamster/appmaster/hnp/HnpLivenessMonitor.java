package com.pivotal.hamster.appmaster.hnp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;

import com.pivotal.hamster.common.HamsterConfig;

abstract public class HnpLivenessMonitor extends AbstractLivelinessMonitor<Object> {
  public static final String MONITOR = "hamster";

  public HnpLivenessMonitor() {
    super(HnpLivenessMonitor.class.getName(), new SystemClock());
  }
  
  public void registerExpire() {
    int expireIntvl = getConfig().getInt(HamsterConfig.HAMSTER_HNP_LIVENESS_EXPIRE_TIME, 
        HamsterConfig.DEFAULT_HAMSTER_HNP_LIVENESS_EXPIRE_TIME);
    setMonitorInterval(expireIntvl / 3);
    setExpireInterval(expireIntvl);
    register(MONITOR);
  }
  
  public void unregisterExpire() {
    unregister(MONITOR);
  }
  
  public void init(Configuration conf) {
    super.init(conf);
  }
  
}

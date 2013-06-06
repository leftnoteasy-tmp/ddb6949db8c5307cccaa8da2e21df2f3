package com.pivotal.hamster.appmaster.hnp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;

import com.pivotal.hamster.appmaster.HamsterConfig;

abstract public class HnpLivenessMonitor extends AbstractLivelinessMonitor<Object> {

  public HnpLivenessMonitor() {
    super(HnpLivenessMonitor.class.getName(), new SystemClock());
  }
  
  public void init(Configuration conf) {
    super.init(conf);
    int expireIntvl = conf.getInt(HamsterConfig.HAMSTER_HNP_LIVENESS_EXPIRE_TIME, 
        HamsterConfig.DEFAULT_HAMSTER_HNP_LIVENESS_EXPIRE_TIME);
    setExpireInterval(expireIntvl);
    setMonitorInterval(expireIntvl / 3);
  }
  
}

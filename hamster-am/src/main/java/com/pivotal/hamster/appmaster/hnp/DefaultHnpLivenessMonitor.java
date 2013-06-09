package com.pivotal.hamster.appmaster.hnp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DefaultHnpLivenessMonitor extends HnpLivenessMonitor {
  private static final Log LOG = LogFactory.getLog(DefaultHnpLivenessMonitor.class);

  @Override
  protected void expire(Object obj) {
    LOG.error(obj.toString() + ", is expired, failed job");
    System.exit(1);
  }

}

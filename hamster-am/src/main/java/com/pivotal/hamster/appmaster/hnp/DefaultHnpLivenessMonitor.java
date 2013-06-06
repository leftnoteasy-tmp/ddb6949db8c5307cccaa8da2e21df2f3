package com.pivotal.hamster.appmaster.hnp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DefaultHnpLivenessMonitor extends HnpLivenessMonitor {
  private static final Log LOG = LogFactory.getLog(DefaultHnpLivenessMonitor.class);

  @Override
  protected void expire(Object arg0) {
    LOG.error("It's a long time that HNP hasn't contact AM, fail AM");
    System.exit(1);
  }

}

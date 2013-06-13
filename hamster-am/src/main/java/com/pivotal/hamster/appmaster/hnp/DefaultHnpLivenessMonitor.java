package com.pivotal.hamster.appmaster.hnp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.Dispatcher;

import com.pivotal.hamster.appmaster.common.HamsterException;
import com.pivotal.hamster.appmaster.event.HamsterFailureEvent;

public class DefaultHnpLivenessMonitor extends HnpLivenessMonitor {
  private static final Log LOG = LogFactory.getLog(DefaultHnpLivenessMonitor.class);
  Dispatcher dispatcher;
  
  public DefaultHnpLivenessMonitor(Dispatcher dispatcher) {
    super();
    this.dispatcher = dispatcher;
  }

  @Override
  protected void expire(Object obj) {
    LOG.error(obj.toString() + ", is expired, failed job");
    dispatcher.getEventHandler().handle(new HamsterFailureEvent(new HamsterException("expired"),
        obj.toString() + ", is expired, failed job"));
  }

}

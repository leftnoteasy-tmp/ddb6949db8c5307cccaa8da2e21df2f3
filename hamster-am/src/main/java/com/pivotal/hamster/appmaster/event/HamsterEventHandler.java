package com.pivotal.hamster.appmaster.event;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;

public class HamsterEventHandler implements EventHandler<HamsterEvent>{
  private static final Log LOG = LogFactory.getLog(HamsterEventHandler.class);

  @Override
  public synchronized void handle(HamsterEvent event) {
    LOG.info("received [" + event.getType().name() + "] event");
    if (event.getType() == HamsterEventType.SUCCEED) {
      LOG.info("complete job");
      System.exit(0);
    } else if (event.getType() == HamsterEventType.FAILURE) {
      HamsterFailureEvent fe = (HamsterFailureEvent)event;
      LOG.error(fe.getDiagnostics() == null ? "null" : fe.getDiagnostics(), fe.getThrowable());
      System.exit(-1);
    }
  }

}

package com.pivotal.hamster.appmaster.event;

import org.apache.hadoop.yarn.event.AbstractEvent;

public class HamsterEvent extends AbstractEvent<HamsterEventType> {

  public HamsterEvent(HamsterEventType type) {
    super(type);
  }

}

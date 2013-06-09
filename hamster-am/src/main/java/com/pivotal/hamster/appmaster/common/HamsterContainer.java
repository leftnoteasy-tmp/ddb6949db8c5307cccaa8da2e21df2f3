package com.pivotal.hamster.appmaster.common;

import org.apache.hadoop.yarn.api.records.Container;

public class HamsterContainer {
  private Container container;
  private ProcessName name;
  
  public HamsterContainer(Container c) {
    this.container = c;
    this.name = null;
  }
  
  public Container getContainer() {
    return container;
  }
  
  public ProcessName getName() {
    return name;
  }
  
  public void setName(ProcessName name) {
    this.name = name;
  }
  
  public boolean isMapped() {
    return name != null;
  }
}

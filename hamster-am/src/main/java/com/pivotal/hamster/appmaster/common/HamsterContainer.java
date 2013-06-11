package com.pivotal.hamster.appmaster.common;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;

public class HamsterContainer {
  private Container container;
  private Resource resource;
  private ProcessName name;
  
  public HamsterContainer(Container c, Resource resource) {
    this.container = c;
    this.resource = resource;
    this.name = null;
  }
  
  public Container getContainer() {
    return container;
  }
  
  public Resource getResource() {
    return resource;
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

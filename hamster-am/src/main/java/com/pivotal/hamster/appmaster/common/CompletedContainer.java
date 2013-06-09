package com.pivotal.hamster.appmaster.common;

public class CompletedContainer {
  int containerId;
  int exitValue;
  
  public CompletedContainer(int containerId, int exitValue) {
    this.containerId = containerId;
    this.exitValue = exitValue;
  }
  
  public int getContainerId() {
    return containerId;
  }
  
  public int getExitValue() {
    return exitValue;
  }
}

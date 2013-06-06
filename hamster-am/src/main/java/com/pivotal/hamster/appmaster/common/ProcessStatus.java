package com.pivotal.hamster.appmaster.common;

public class ProcessStatus {
  ProcessName name;
  int exitValue;
  
  public ProcessStatus(ProcessName name, int exitValue) {
    this.name = name;
    this.exitValue = exitValue;
  }
  
  public ProcessName getName() {
    return name;
  }
  
  public int getExitValue() {
    return exitValue;
  }
}

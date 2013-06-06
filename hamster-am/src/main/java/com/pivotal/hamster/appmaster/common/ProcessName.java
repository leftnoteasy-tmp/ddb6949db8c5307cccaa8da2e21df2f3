package com.pivotal.hamster.appmaster.common;

public class ProcessName {
  int jobId;
  int vpId;
  static final int BIG_PRIME = 502357;
  
  @Override
  public int hashCode() {
    return jobId * BIG_PRIME + vpId;
  }
}

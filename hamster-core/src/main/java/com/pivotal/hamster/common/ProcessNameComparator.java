package com.pivotal.hamster.common;

import java.util.Comparator;

public class ProcessNameComparator implements Comparator<ProcessName> {

  @Override
  public int compare(ProcessName left, ProcessName right) {
    if (left == null || right == null) {
      return 0;
    }
    
    if (left.jobId != right.jobId) {
      return left.jobId - right.jobId;
    }
    
    return left.vpId - right.vpId;
  }

}

package com.pivotal.hamster.common;

import org.apache.hadoop.yarn.YarnException;

public class HamsterException extends YarnException {
  private static final long serialVersionUID = -4497338225113444073L;

  public HamsterException(String message) {
    super(message);
  }
  
  public HamsterException(Throwable t) {
    super(t);
  }

}

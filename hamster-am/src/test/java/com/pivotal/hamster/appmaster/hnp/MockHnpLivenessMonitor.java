package com.pivotal.hamster.appmaster.hnp;

public class MockHnpLivenessMonitor extends HnpLivenessMonitor {
  boolean expired = false;
  
  
  @Override
  protected void expire(Object arg0) {
    expired = true;
  }
  
  public boolean getExpired() {
    return expired;
  }

}

package com.pivotal.hamster.appmaster.hnp;

import org.apache.hadoop.yarn.service.AbstractService;

abstract public class HnpService extends AbstractService {
  public static final String HANDSHAKE_MSG = "hamster-001";
  public static final byte SUCCEED = 1;
  public static final byte FAILED = 2;
  
  public static enum HnpState {
    Init,
    Registered,
    Allocated,
    Finished
  }
  HnpState state = HnpState.Init;

  public HnpService(String name) {
    super(HnpService.class.getName());
  }
  
  abstract int getServerPort();
  
  HnpState getHnpState() {
    return state;
  }
}

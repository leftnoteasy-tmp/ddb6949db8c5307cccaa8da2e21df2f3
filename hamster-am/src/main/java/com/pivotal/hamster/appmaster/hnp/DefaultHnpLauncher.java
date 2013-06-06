package com.pivotal.hamster.appmaster.hnp;

public class DefaultHnpLauncher extends HnpLauncher {
  public HnpService hnpService;
  
  public DefaultHnpLauncher(HnpService hnpService) {
    super(DefaultHnpLauncher.class.getName());
    this.hnpService = hnpService;
  }

}

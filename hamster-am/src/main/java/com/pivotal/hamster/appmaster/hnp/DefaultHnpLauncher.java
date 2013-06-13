package com.pivotal.hamster.appmaster.hnp;

import org.apache.hadoop.yarn.event.Dispatcher;

public class DefaultHnpLauncher extends HnpLauncher {
  HnpService hnpService;
  Dispatcher dispatcher;
  
  public DefaultHnpLauncher(Dispatcher dispatcher, HnpService hnpService) {
    super(DefaultHnpLauncher.class.getName());
    this.hnpService = hnpService;
  }

}

package com.pivotal.hamster.appmaster.launcher;

import com.pivotal.hamster.appmaster.hnp.HnpLivenessMonitor;
import com.pivotal.hamster.proto.HamsterProtos.LaunchRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.LaunchResponseProto;

public class YarnContainerLauncher extends ContainerLauncher {
  private HnpLivenessMonitor mon;
  
  public YarnContainerLauncher(HnpLivenessMonitor mon) {
    super(YarnContainerLauncher.class.getName());
    this.mon = mon;
  }

  @Override
  public LaunchResponseProto launch(LaunchRequestProto request) {
    return null;
  }

}

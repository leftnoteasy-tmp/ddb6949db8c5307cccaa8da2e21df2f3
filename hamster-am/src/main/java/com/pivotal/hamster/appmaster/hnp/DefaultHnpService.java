package com.pivotal.hamster.appmaster.hnp;

import com.pivotal.hamster.appmaster.allocator.ContainerAllocator;
import com.pivotal.hamster.appmaster.launcher.ContainerLauncher;

public class DefaultHnpService extends HnpService {
  ContainerAllocator containerAllocator;
  ContainerLauncher containerLauncher;
  HnpLivenessMonitor mon;
  
  public DefaultHnpService(ContainerAllocator containerAllocator, 
      ContainerLauncher containerLauncher, HnpLivenessMonitor mon) {
    super(DefaultHnpService.class.getName());
    this.containerAllocator = containerAllocator;
    this.containerLauncher = containerLauncher;
    this.mon = mon;
  }
}

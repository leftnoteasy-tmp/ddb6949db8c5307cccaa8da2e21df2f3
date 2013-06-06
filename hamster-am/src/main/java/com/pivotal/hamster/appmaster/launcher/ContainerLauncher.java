package com.pivotal.hamster.appmaster.launcher;

import org.apache.hadoop.yarn.service.AbstractService;

import com.pivotal.hamster.appmaster.common.LaunchContext;
import com.pivotal.hamster.proto.HamsterProtos;

abstract public class ContainerLauncher extends AbstractService {
  public ContainerLauncher(String name) {
    super(name);
  }

  abstract public boolean[] launch(LaunchContext[] request);  
}

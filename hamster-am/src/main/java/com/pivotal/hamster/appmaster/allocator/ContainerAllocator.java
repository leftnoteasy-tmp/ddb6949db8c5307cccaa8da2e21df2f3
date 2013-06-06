package com.pivotal.hamster.appmaster.allocator;

import org.apache.hadoop.yarn.service.AbstractService;

import com.pivotal.hamster.appmaster.hnp.HnpLivenessMonitor;
import com.pivotal.hamster.proto.HamsterProtos;

abstract public class ContainerAllocator extends AbstractService {
  private HnpLivenessMonitor mon;
  
  public ContainerAllocator(String name) {
    super(name);
  }
  
  abstract public HamsterProtos.AllocateResponseProto allocate(HamsterProtos.AllocateRequestProto request);
  
  abstract public HamsterProtos.HeartbeatResponseProto pullCompletedContainers(HamsterProtos.HeartbeatRequestProto request);

}

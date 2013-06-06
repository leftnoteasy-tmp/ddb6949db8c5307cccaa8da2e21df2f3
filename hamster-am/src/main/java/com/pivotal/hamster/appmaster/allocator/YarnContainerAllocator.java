package com.pivotal.hamster.appmaster.allocator;

import com.pivotal.hamster.appmaster.hnp.HnpLivenessMonitor;
import com.pivotal.hamster.proto.HamsterProtos.AllocateRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.AllocateResponseProto;
import com.pivotal.hamster.proto.HamsterProtos.HeartbeatRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.HeartbeatResponseProto;

public class YarnContainerAllocator extends ContainerAllocator {
  private HnpLivenessMonitor mon;

  public YarnContainerAllocator(HnpLivenessMonitor mon) {
    super(YarnContainerAllocator.class.getName());
    this.mon = mon;
  }

  @Override
  public AllocateResponseProto allocate(AllocateRequestProto request) {
    return null;
  }

  @Override
  public HeartbeatResponseProto pullCompletedContainers(
      HeartbeatRequestProto request) {
    // TODO Auto-generated method stub
    return null;
  }

}

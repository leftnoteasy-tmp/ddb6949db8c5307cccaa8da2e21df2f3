package com.pivotal.hamster.appmaster.allocator;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.junit.Test;

public class ProbabilityBasedAllocationStrategyTest {
  class MockYarnContainerAllocator extends YarnContainerAllocator {
    AllocateResponse response = null;
    List<ResourceRequest> request = null;
    int round = 0;
    
    public MockYarnContainerAllocator() {
      super(null);
    }
    
    @Override 
    AllocateResponse invokeAllocate(List<ResourceRequest> resourceRequests) {
      return null;
    }
  }
  
  class Allocator_testAllocation1 extends MockYarnContainerAllocator {

    @Override 
    AllocateResponse invokeAllocate(List<ResourceRequest> resourceRequests) {
      // check round 1, round 2, round 3, and mock result
      return null;
    }
  }
  
  @Test
  public void testAllocation1() {
    Allocator_testAllocation1 allocator = new Allocator_testAllocation1();
    ProbabilityBasedAllocationStrategy strategy = new ProbabilityBasedAllocationStrategy(allocator, false);
    ConcurrentLinkedQueue<ContainerId> releaseContainers = new ConcurrentLinkedQueue<ContainerId>();
    strategy.allocate(5, releaseContainers, null);
  }
}

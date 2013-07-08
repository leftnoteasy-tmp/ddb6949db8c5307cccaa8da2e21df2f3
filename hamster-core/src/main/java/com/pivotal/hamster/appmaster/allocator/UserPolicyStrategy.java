package com.pivotal.hamster.appmaster.allocator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

import com.pivotal.hamster.common.HamsterContainer;

public class UserPolicyStrategy implements AllocationStrategy {
  public UserPolicyStrategy(ContainerAllocator allocator, boolean verbose) {
    
  }
  
  @Override
  public Map<String, List<HamsterContainer>> allocate(int n,
      ConcurrentLinkedQueue<ContainerId> releaseContainers, Resource resource, Configuration conf) {
    // TODO Auto-generated method stub
    return null;
  }

}

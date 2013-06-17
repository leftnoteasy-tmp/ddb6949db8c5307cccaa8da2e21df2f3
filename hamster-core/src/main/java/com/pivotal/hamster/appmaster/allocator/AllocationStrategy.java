package com.pivotal.hamster.appmaster.allocator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

import com.pivotal.hamster.common.HamsterContainer;

public interface AllocationStrategy {
  public Map<String, List<HamsterContainer>> allocate(int n, 
      ConcurrentLinkedQueue<ContainerId> releaseContainers,
      Resource resource);
}

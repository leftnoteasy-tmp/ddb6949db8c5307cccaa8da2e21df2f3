package com.pivotal.hamster.appmaster.allocator;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.service.AbstractService;

import com.pivotal.hamster.common.CompletedContainer;
import com.pivotal.hamster.common.HamsterContainer;
import com.pivotal.hamster.common.HamsterException;

abstract public class ContainerAllocator extends AbstractService {  
  public ContainerAllocator(String name) {
    super(name);
  }
  
  abstract public Map<String, List<HamsterContainer>> allocate(int n) throws HamsterException;
  
  abstract public CompletedContainer[] pullCompletedContainers();
  
  abstract public void completeHnp(FinalApplicationStatus status);
  
}

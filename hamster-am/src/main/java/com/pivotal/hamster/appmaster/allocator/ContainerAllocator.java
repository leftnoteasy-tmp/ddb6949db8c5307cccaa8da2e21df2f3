package com.pivotal.hamster.appmaster.allocator;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.service.AbstractService;

import com.pivotal.hamster.appmaster.common.ProcessName;
import com.pivotal.hamster.appmaster.common.ProcessStatus;

abstract public class ContainerAllocator extends AbstractService {  
  public ContainerAllocator(String name) {
    super(name);
  }
  
  abstract public Map<ProcessName, Container> allocate(int n);
  
  abstract public ProcessStatus[] pullCompletedContainers();
  
  abstract public void completeHnp(FinalApplicationStatus status);
  
}

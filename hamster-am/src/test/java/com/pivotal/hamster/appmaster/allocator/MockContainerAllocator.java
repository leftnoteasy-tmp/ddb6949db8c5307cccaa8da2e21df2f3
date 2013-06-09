package com.pivotal.hamster.appmaster.allocator;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.pivotal.hamster.appmaster.common.HamsterContainer;
import com.pivotal.hamster.appmaster.common.HamsterException;
import com.pivotal.hamster.appmaster.common.CompletedContainer;

public class MockContainerAllocator extends ContainerAllocator {
  Map<String, List<HamsterContainer>> allocateResult;
  List<CompletedContainer> completedContainers;

  public MockContainerAllocator() {
    super("mock-allocator");
  }
  
  public void setAllocateResult(Map<String, List<HamsterContainer>> allocateResult) {
    this.allocateResult = allocateResult;
  }
  
  public void setCompletedContainer(List<CompletedContainer> containers) {
    completedContainers = containers;
  }

  @Override
  public Map<String, List<HamsterContainer>> allocate(int n)
      throws HamsterException {
    return allocateResult;
  }

  @Override
  public CompletedContainer[] pullCompletedContainers() {
    CompletedContainer[] c = completedContainers.toArray(new CompletedContainer[0]);
    completedContainers.clear();
    return c;
  }

  @Override
  public void completeHnp(FinalApplicationStatus status) {

  }
}

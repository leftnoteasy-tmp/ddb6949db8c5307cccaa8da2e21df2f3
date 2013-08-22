package com.pivotal.hamster.appmaster.allocator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

public class NaiveAllocationStrategy extends AllocationStrategyBase {
  private static final Log LOG = LogFactory.getLog(NaiveAllocationStrategy.class);
  Set<String> availableHosts;

  public NaiveAllocationStrategy(ContainerAllocator allocator, boolean verbose) {
    super(allocator, verbose);
    availableHosts = new HashSet<String>();
  }

  @Override
  List<ResourceRequest> getAskList() {
    int askCount = n - m;
    List<ResourceRequest> askList = new ArrayList<ResourceRequest>();
    ResourceRequest req = recordFactory.newRecordInstance(ResourceRequest.class);
    Priority pri = recordFactory.newRecordInstance(Priority.class);
    pri.setPriority(DEFAULT_PRIORITY);
    req.setCapability(resource);
    req.setHostName(ANY);
    req.setNumContainers(askCount);
    req.setPriority(pri);
    askList.add(req);
    
    if (verbose) {
      LOG.info(String.format("add %d resource request with host=%s, pri=%d",
          askCount, ANY, DEFAULT_PRIORITY));
    }
    
    return askList;
  }
}

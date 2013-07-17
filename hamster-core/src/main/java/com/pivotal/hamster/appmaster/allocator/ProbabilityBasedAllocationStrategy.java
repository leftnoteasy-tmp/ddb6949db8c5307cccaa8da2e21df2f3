package com.pivotal.hamster.appmaster.allocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.RackResolver;

import com.pivotal.hamster.common.HamsterException;

public class ProbabilityBasedAllocationStrategy extends AllocationStrategyBase {  
  private static final Log LOG = LogFactory.getLog(ProbabilityBasedAllocationStrategy.class);
  
  public ProbabilityBasedAllocationStrategy(ContainerAllocator allocator, boolean verbose) {
    super(allocator, verbose);
  }
  
  void returnAllContainersInHostId(int hostId) {
    List<Container> containers = hostIdToContainers.get(hostId);
    for (Container c : containers) {
      releaseContainers.add(c.getId());
    }
    
    // remove entry in hostToId
    String host = null;
    for (Entry<String, Integer> entry : hostToId.entrySet()) {
      if (entry.getValue() == hostId) {
        if (host != null) {
          // double check if host-id is unique
          LOG.error("host-id is not unique, please check");
          throw new HamsterException("host-id is not unique, please check");
        }
        host = entry.getKey();
      }
    }
    if (host == null) {
      LOG.error("failed to find a host with id=" + hostId);
      throw new HamsterException("failed to find a host with id=" + hostId);
    }
    hostToId.remove(host);
    
    containers.clear();
  }
  
  void returnPartialContainersInHostId(int hostId, int count) {
    List<Container> containers = hostIdToContainers.get(hostId);
    List<Container> newContainers = new ArrayList<Container>();
    int size = containers.size();
    for (int i = size - count; i < size; i++) {
      releaseContainers.add(containers.get(i).getId());
    }
    for (int i = 0; i < size - count; i++) {
      newContainers.add(containers.get(i));
    }
    hostIdToContainers.put(hostId, newContainers);
  }
  
  @Override
  void releaseRedundantContainers() {
    int nNeedRelease = m - n;

    // get host id to counts, we will release host will less containers first
    HostIdToCount[] hostIdToCounts = new HostIdToCount[hostIdToContainers
        .size() - 1];
    int idx = 0;
    for (Entry<Integer, List<Container>> entry : hostIdToContainers.entrySet()) {
      if (entry.getKey() != 0) {
        hostIdToCounts[idx] = new HostIdToCount(entry.getKey(), entry
            .getValue().size());
        idx++;
      }
    }

    // sort host by number of containers;
    Arrays.sort(hostIdToCounts, new Comparator<HostIdToCount>() {
      @Override
      public int compare(HostIdToCount left, HostIdToCount right) {
        return left.count - right.count;
      }
    });

    for (int i = 0; i < hostIdToCounts.length; i++) {
      HostIdToCount tmp = hostIdToCounts[i];
      if (tmp.count <= 1) {
        returnAllContainersInHostId(tmp.hostId);
        LOG.info("release all containers in host:[" + tmp.hostId
            + "], container count = " + tmp.count);
      } else if (nNeedRelease > 0) {
        if (nNeedRelease >= tmp.count - 1) {
          returnAllContainersInHostId(tmp.hostId);
          nNeedRelease -= (tmp.count - 1);
          LOG.info("release all containers in host:[" + tmp.hostId
              + "], container count = " + tmp.count);
        } else {
          returnPartialContainersInHostId(tmp.hostId, nNeedRelease);
          nNeedRelease = 0;
          LOG.info("release some containers in host:[" + tmp.hostId
              + "], container count = " + nNeedRelease
              + " left-container-count=" + (tmp.count - nNeedRelease));
        }
      }
    }

    LOG.info("STATISTIC: Container Utilization = " + (m - releaseContainers.size()) / (float)m );
    
    // check if we need release container in host-0
    if (nNeedRelease > 0) {
      List<Container> containerList = hostIdToContainers.get(0);
      if ((containerList == null) || (containerList.size() <= nNeedRelease)) {
        throw new HamsterException(
            "try to release containers in host-0, but containers in host-0 are not enough to release");
      }
      returnPartialContainersInHostId(0, nNeedRelease);
      LOG.info("release some containers in host:[0], container count = "
          + nNeedRelease + " left-container-count="
          + (containerList.size() - nNeedRelease));
    }
  }
  
  @Override
  List<ResourceRequest> getAskList() {
    int x; // number of containers need to be asked in this turn
    int y; // number of containers already put into ask list
    // resoure request divided by hosts
    Map<String, List<Integer>> resourceRequests = new HashMap<String, List<Integer>>();
    
    x = n - m;
    y = 0;
    
    // first we need get how many host we will expand
    int nExpandHosts = getNExpandHosts();
    if (!resourceRequests.containsKey(ANY)) {
      resourceRequests.put(ANY, new ArrayList<Integer>());
    }
    resourceRequests.get(ANY).add(nExpandHosts + x);
    
    // calculate prob array
    HostIdToProb[] p = new HostIdToProb[nHosts];
    double pSum = 0;
    for (Entry<String, Integer> entry : hostToId.entrySet()) {
      int hostId = entry.getValue();
      p[hostId] = new HostIdToProb(entry.getKey(), 0);
      if (hostId == 0) {
        p[hostId].prob = 1; // we need to consider hnp slot our self
      }
      
      if (hostIdToContainers.containsKey(hostId)) {
        List<Container> containerList = hostIdToContainers.get(hostId);
        if (containerList != null) {
          p[hostId].prob += containerList.size();
        } else {
          LOG.error("get a null container list, this shouldn't happen");
          throw new HamsterException("get a null container list, this shouldn't happen");
        }
      }
      
      if (p[hostId].prob > 1e-8) {
        p[hostId].prob = 1 / p[hostId].prob;
        pSum += p[hostId].prob;
      }
    }
    
    if (pSum < 1e-8) {
      LOG.error("get an error sum of total p, this shouldn't happen");
      throw new HamsterException("get an error sum of total p, this shouldn't happen");
    }
    
    // normalize p
    for (int i = 0; i < nHosts; i++) {
      if (p[i].prob > 1e-8) {
        p[i].prob = p[i].prob / pSum;
      } else {
        p[i].prob = -1;
      }
    }
    
    // sort p in reverse order of prob
    Arrays.sort(p, new Comparator<HostIdToProb>() {
      @Override
      public int compare(HostIdToProb left, HostIdToProb right) {
        if (left.prob < right.prob) {
          return 1;
        } else if (left.prob > right.prob) {
          return -1;
        }
        return 0;
      }
    });
    
    for (int i = 0; i < nHosts; i++) {
      int askCount = (int) Math.round(Math.ceil(p[i].prob * x) + 1e-8);
      if (askCount <= 0) {
        askCount = 1;
      }
      
      // add node local request
      List<Integer> hostAskList = resourceRequests.get(p[i].host);
      if (null == hostAskList) {
        hostAskList = new ArrayList<Integer>();
        resourceRequests.put(p[i].host, hostAskList);
        hostAskList.add(askCount);
      } else {
        hostAskList.set(0, hostAskList.get(0) + askCount);
      }
      
      // add rack local request
      String rack = RackResolver.resolve(p[i].host).getNetworkLocation();
      hostAskList = resourceRequests.get(rack);
      if (null == hostAskList) {
        hostAskList = new ArrayList<Integer>();
        hostAskList.add(askCount);
        resourceRequests.put(rack, hostAskList);
      } else {
        hostAskList.set(0, hostAskList.get(0) + askCount);
      }
      
      y += askCount;
      if (y >= x) {
        break;
      }
    }
    
    // we will assemble List<ResourceRequest>
    List<ResourceRequest> ret = new ArrayList<ResourceRequest>();
    for (Entry<String, List<Integer>> entry : resourceRequests.entrySet()) {
      for (int i = 0; i < entry.getValue().size(); i++) {
        int slotNum = entry.getValue().get(i);
        ResourceRequest req = recordFactory.newRecordInstance(ResourceRequest.class);
        Priority pri = recordFactory.newRecordInstance(Priority.class);
        pri.setPriority(DEFAULT_PRIORITY);
        req.setCapability(resource);
        req.setHostName(entry.getKey());
        req.setNumContainers(slotNum);
        req.setPriority(pri);
        ret.add(req);
      }
    }
    
    return ret;
  }
  
  int getNExpandHosts() {
    // if user specify nProcPerHost, we will use what user specified
    int nProcPerHost = System.getenv().get("HAMSTER_NPROC_PER_NODE") == null ? -1
        : Integer.parseInt("HAMSTER_NPROC_PER_NODE");
    int expandHost = -1;
    if (nProcPerHost > 0) {
      if (n / nProcPerHost + 1 > nHosts) {
        return n / nProcPerHost + 1 - nHosts;
      }
    }
    
    // if user don't specify, but we already hold a lot of hosts, we will make this slower
    if (nHosts >= n) {
      return 1;
    }
    
    // else, we will use our function to calculate number of nodes we will expand:
    // ((1 / (e^(x - 4) + 1)) * 0.3 + 0.1)
    double p = (1 / (Math.exp(nHosts - 4) + 1)) * 0.3 + 0.1;
    expandHost = (int) Math.round(p * nHosts);
    if (expandHost <= 0) {
      expandHost = 1;
    }
    
    return expandHost;
  }
}

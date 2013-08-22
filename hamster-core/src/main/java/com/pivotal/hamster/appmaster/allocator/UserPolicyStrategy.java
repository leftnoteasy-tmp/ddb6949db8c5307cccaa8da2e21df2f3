package com.pivotal.hamster.appmaster.allocator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.RackResolver;

import com.pivotal.hamster.appmaster.utils.HamsterAppMasterUtils;
import com.pivotal.hamster.common.HamsterConfig;
import com.pivotal.hamster.common.HamsterException;

public class UserPolicyStrategy extends AllocationStrategyBase {
  private static final Log LOG = LogFactory.getLog(UserPolicyStrategy.class);

  // null if no hosts specified
  Set<String> availableHosts = null;
  boolean initialized = false;
  int mproc;
  int mnode;
  
  public UserPolicyStrategy(ContainerAllocator allocator, boolean verbose) {
    super(allocator, verbose);
  }
  
  void init() {
    // initialize host list
    String hostlist = conf.get(HamsterConfig.USER_POLICY_HOST_LIST_KEY);
    if (null != hostlist) {
      availableHosts = new HashSet<String>();
      int hostId = 1;
      for (String host : hostlist.split(",")) {
        if (!host.isEmpty()) {
          String normalizedHost = HamsterAppMasterUtils.normlizeHostName(host);
          availableHosts.add(normalizedHost);
          LOG.info("add predefined host:[" + normalizedHost + "] to known hosts.");
          if (!hostToId.containsKey(normalizedHost)) {
            hostToId.put(normalizedHost, hostId);
            hostId++;
          }
        }
      }
    }
    
    // initialize mproc, mnode
    mproc = conf.getInt(HamsterConfig.USER_POLICY_MPROC_KEY, Short.MAX_VALUE);
    mnode = conf.getInt(HamsterConfig.USER_POLICY_MNODE_KEY, Short.MAX_VALUE);
    
    // sanity check if user specified hosts_num * mproc >= n
    if (n > availableHosts.size() * mproc) {
      String msg = String.format("user specified host count=%d, mproc=%d, the total number of slots in theory for this job=%d, but mpirun np=%d, it's not possbile to allocate np=%d",
          availableHosts.size(), mproc, availableHosts.size() * mproc, n, n);
      LOG.error(msg);
      throw new HamsterException(msg);
    }
    
    initialized = true;
  }
  
  /**
   * get upper_bound(x / y)
   */
  int getDivisionUpperBound(int x, int y) {
    int r = x / y;
    if (x % y == 0) {
      return r;
    }
    
    return r + 1;
  }
  
  int getMinimumY() {
    // define left/right bound of y
    int ly = 1;
    int ry = Math.min(getDivisionUpperBound(n, availableHosts.size()), mproc);
    int miny = Integer.MAX_VALUE;
    
    while (ly <= ry) {
      int mid = (ly + ry) >> 1;
      int sum = 0;
      
      // caculate total size based on mid
      for (String host : availableHosts) {
        int containersCount = getContainersCountForMpiProc(host);
        if (containersCount < mid) {
          sum += mid;
        } else {
          sum += containersCount;
        }
      }
      
      if (sum > n) {
        ry--;
        // update miny
        if (miny > mid) {
          miny = mid;
        }
      } else if (sum < n) {
        ly++;
      } else {
        return mid;
      }
    }
    
    return miny;
  }
  
  /*
   * add resource requests for a specified host, we will add rack, ANY if needed
   */
  void addResourceRequestForNode(Map<String, Integer> resourceRequests, String host, int askCount) {
    // add node request
    addResourceRequests(resourceRequests, host, askCount);
    
    // add rack request
    String rack = RackResolver.resolve(host).getNetworkLocation();
    addResourceRequests(resourceRequests, rack, askCount);
    
    // add any request
    addResourceRequests(resourceRequests, ANY, askCount);
  }

  @Override
  List<ResourceRequest> getAskList() {
    if (!initialized) {
      init();
    }
    
    if (null == availableHosts || mnode < Short.MAX_VALUE) {
      LOG.error("we didn't support not specified availableHosts / specified mnode now");
      throw new HamsterException("we didn't support not specified availableHosts / specified mnode now");
    }
    
    // we will use a binary search method to search how deep water we put can get total volumn of water
    // assume we have n barrel, each barrel has already some water in it, e.g. x_1, x_2, ... x_n
    // x_i is the volumn of water in barrel i, we need m total volumn of water
    // we need get a mimimum height y, that can make n * y + (sum of volumns higher than y already) >= m
    // we can use binary search y (1 <= y <= min(upper_bound(m/n), mproc))
    int miny = getMinimumY();
    
    // requests list
    Map<String, Integer> resourceRequests = new HashMap<String, Integer>();
    
    // make allocation list based on miny, we will fill all waters in barrel_i below miny to miny 
    // (the add count will be (miny - x_i))
    // and we will add 1 ask to each barrel with x_i >= miny but < mproc to avoid deadlocking
    for (String host : availableHosts) {
      int containersCount = getContainersCountForMpiProc(host);
      if (containersCount < miny) {
        int askCount = getLackingContainersCount(host, miny);
        addResourceRequestForNode(resourceRequests, host, askCount);
      } else {
        if (containersCount < mproc) {
          addResourceRequestForNode(resourceRequests, host, 1);
        }
      }
    }
    
    // we will assemble List<ResourceRequest>
    List<ResourceRequest> ret = new ArrayList<ResourceRequest>();
    for (Entry<String, Integer> entry : resourceRequests.entrySet()) {
      int slotNum = entry.getValue();
      ResourceRequest req = recordFactory
          .newRecordInstance(ResourceRequest.class);
      Priority pri = recordFactory.newRecordInstance(Priority.class);
      pri.setPriority(DEFAULT_PRIORITY);
      req.setCapability(resource);
      req.setHostName(entry.getKey());
      req.setNumContainers(slotNum);
      req.setPriority(pri);
      ret.add(req);
    }
    
    return ret;
  }
  
  private void putContainersToMpiProcSizeMap(int mpiProc,
      List<Container> containers, Map<Integer, List<List<Container>>> map) {
    if (map.get(mpiProc) == null) {
      map.put(mpiProc, new ArrayList<List<Container>>());
    }
    map.get(mpiProc).add(containers);
  }

  @Override
  void releaseRedundantContainers() {
    int nNeedRelease = m - n;
    
    // there're two steps to release redundant containers,
    // first, we will scan all allocated containers, if containers count in one host are <= 1 (0 for hnp node)
    // we will discard such containers
    // then we will deduct containers from more to less
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
    
    // loop hosts and release container cannot run MPI proc
    for (String host : availableHosts) {
      List<Container> containers = getContainersByHost(host);
      int mpiProc = getContainersCountForMpiProc(host);
      if ((containers != null) && (containers.size() > 0) && (mpiProc <= 0)) {
        for (Container container : containers) {
          releaseContainers.add(container.getId());
        }
        containers.clear();
      }
    }
    
    // we will put all containers to a map<int, List<List<containers>>
    // it's <mpi_proc_size, <container-list-host_a, container_list_host_b, ...>>
    Map<Integer, List<List<Container>>> nodeSizeToContainersLists = new HashMap<Integer, List<List<Container>>>();
    for (String host : availableHosts) {
      List<Container> containers = getContainersByHost(host);
      int mpiProc = getContainersCountForMpiProc(host);
      
      // skip such node
      if (mpiProc == 0) {
        // sanity check
        if (containers != null && !containers.isEmpty()){
          LOG.error("this shouldn't happen, if #mpi-proc > 0 in one node, the container list should be empty");
          throw new HamsterException("this shouldn't happen, if #mpi-proc > 0 in one node, the container list should be empty");
        }
        continue;
      }
      
      // sanity check
      if (mpiProc > 0 && (containers == null || containers.isEmpty())) {
        LOG.error("this shouldn't happen, all node with mpi_proc > 0 should have enough containers");
        throw new HamsterException("this shouldn't happen, all node with mpi_proc > 0 should have enough containers");
      }
      
      putContainersToMpiProcSizeMap(mpiProc, containers, nodeSizeToContainersLists);
    }
    
    // we will return containers from reversed order
    int maxMpiProcCount = -1;
    for (Integer key : nodeSizeToContainersLists.keySet()) {
      if (key > maxMpiProcCount) {
        maxMpiProcCount = key;
      }
    }
    
    int key = maxMpiProcCount;
    while (key > 0 && nNeedRelease > 0) {
      List<List<Container>> containersLists = nodeSizeToContainersLists.get(key);
      if (null == containersLists) {
        key--;
        continue;
      }
      for (int i = containersLists.size() - 1; i >= 0; i--) {
        if (nNeedRelease <= 0) {
          break;
        }
        List<Container> containers = containersLists.get(i);
        // if we have only one container for mpi proc on this node, and this
        // container will be return, we will return all containers in this node
        if (key == 1) {
          for (Container container : containers) {
            releaseContainers.add(container.getId());
          }
        } else {
          releaseContainers.add(containers.get(containers.size() - 1).getId());
          containers.remove(containers.size() - 1);
        }
        // we removed one container in this host, so it can launch key - 1
        // mpi_proc, we will add it to level below
        containersLists.remove(i);
        if (key > 1) {
          putContainersToMpiProcSizeMap(key - 1, containers, nodeSizeToContainersLists);
        }
        // update nNeedRelease
        nNeedRelease--;
      }
      key--;
    }
  }

  @Override
  // check host is available during allocation
  boolean checkIsHostAvailable(String host) {
    if (availableHosts != null && !availableHosts.contains(host)) {
      if (verbose) {
        LOG.info("received host not in user specified hostlist, host:" + host);
      }
      return false;
    }
    if (getContainersCountForMpiProc(host) >= mproc) {
      if (verbose) {
        LOG.info("this host already have enough containers, discard this one, host:" + host);
      }
      return false;
    }
    return true;
  }
}

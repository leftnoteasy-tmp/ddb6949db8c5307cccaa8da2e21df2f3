package com.pivotal.hamster.appmaster.allocator;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import com.pivotal.hamster.appmaster.utils.HamsterAppMasterUtils;
import com.pivotal.hamster.common.HamsterContainer;
import com.pivotal.hamster.common.HamsterException;

abstract public class AllocationStrategyBase implements AllocationStrategy {
  private static final Log LOG = LogFactory.getLog(AllocationStrategyBase.class);

  static final String ANY = "*";
  static final int DEFAULT_PRIORITY = 20;
  
  // how many MPI proc we need to execute
  int n;
  // how many slots we already got to execute MPI proc
  int m;
  final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  ConcurrentLinkedQueue<ContainerId> releaseContainers;
  Resource resource;
  boolean verbose;
  // internal data structure for \vec{a} and \vec{h}
  Map<String, Integer> hostToId;
  Map<Integer, List<Container>> hostIdToContainers;
  int nHosts = 0;
  YarnContainerAllocator allocator;
  Configuration conf;
  
  static class HostIdToProb {
    String host;
    double prob;
    
    HostIdToProb(String host, double prob) {
      this.host = host;
      this.prob = prob;
    }
  }
  
  static class HostIdToCount {
    int hostId;
    int count;
    
    HostIdToCount(int hostId, int count) {
      this.hostId = hostId;
      this.count = count;
    }
  }
  
  public AllocationStrategyBase(ContainerAllocator allocator, boolean verbose) {
    this.verbose = verbose;
    this.allocator = (YarnContainerAllocator)allocator;
  }
  
  @Override
  public Map<String, List<HamsterContainer>> allocate(int n,
      ConcurrentLinkedQueue<ContainerId> releaseContainers, Resource resource, Configuration conf)
      throws HamsterException {
    // init local variables
    this.n = n;
    this.m = 0;
    this.conf = conf;
    this.releaseContainers = releaseContainers;
    this.resource = resource;

    // init internal data structure
    hostToId = new HashMap<String, Integer>();
    hostIdToContainers = new HashMap<Integer, List<Container>>();

    try {
      // add local node to set
      String local = HamsterAppMasterUtils.getNormalizedLocalhost();
      hostToId.put(local, nHosts);
      hostIdToContainers.put(0, new ArrayList<Container>());
      nHosts++;

      return internalAllocate();
    } catch (YarnRemoteException e) {
      throw new HamsterException(e);
    } catch (UnknownHostException e) {
      throw new HamsterException(e);
    } catch (InterruptedException e) {
      throw new HamsterException(e);
    }
  }
  
  Map<String, List<HamsterContainer>> internalAllocate() throws YarnRemoteException, UnknownHostException, InterruptedException {
    int round = 0;
    
    while (n > m) {
      // get next ask list
      List<ResourceRequest> askList = getAskList();
      
      if (verbose) {
        printAskList(round, askList);
      }
      
      // assemble request and send to AM
      int newSlots = makeRemoteRequest(askList);
      m += newSlots;
      
      if (newSlots > 0 && verbose) {
        LOG.info(String.format("get %d new slots from RM, now we have %d", newSlots, m));
      }
      
      round++;
      
      // sleep for a while before next ask
      Thread.sleep(200);
    }
    
    if (verbose) {
      LOG.info("STATISTIC: Iterations = " + round );
    }
    
    // release extra containers
    releaseRedundantContainers();
    
    Map<String, List<HamsterContainer>> nodeContainerMap = assembleAllocationResult();
    
    if (verbose) {
      LOG.info("STATISTIC: Nodes = " + nodeContainerMap.size());
    }
    return nodeContainerMap;
  }
  
  /*
   * get allocated containers in a specified host
   */
  List<Container> getContainersByHost(String host) {
    if (hostToId.containsKey(host)) {
      int id = hostToId.get(host);
      return hostIdToContainers.get(id);
    }
    return null;
  }
  
  /*
   * add resource request to existing map, key can be a hostname, rackname or *
   */
  void addResourceRequests(Map<String, Integer> resourceRequests, String key, int count) {
    if (resourceRequests.containsKey(key)) {
      resourceRequests.put(key, resourceRequests.get(key) + count);
    } else {
      resourceRequests.put(key, count);
    }
  }
  
  /*
   * get containers count for MPI proc and daemon
   */
  int getContainersCount(String host) {
    List<Container> containers = getContainersByHost(host);
    if (null == containers) {
      return 0;
    }
    return containers.size();
  }
  
  /*
   * get containers count for MPI proc (deducted daemon's usage)
   */
  int getContainersCountForMpiProc(String host) {
    int containersCount = getContainersCount(host);
    if (containersCount > 0) {
      int id = hostToId.get(host);
      if (id != 0) {
        return containersCount - 1;
      }
    }
    return containersCount;
  }
  
  /*
   * get lacking containers count for host to run n mpi proc
   */
  int getLackingContainersCount(String host, int n) {
    int containersCount = getContainersCount(host);
    int id = hostToId.get(host);
    if (id != 0) {
      return Math.max(0, n - containersCount + 1);
    } else {
      return Math.max(0, n - containersCount);
    }
  }
  
  Map<String, List<HamsterContainer>> assembleAllocationResult() {
    Map<String, List<HamsterContainer>> result = new HashMap<String, List<HamsterContainer>>();
    int total = 0;
    
    // loop allocation and double check if allocation/release are correct
    for (Entry<String, Integer> entry : hostToId.entrySet()) {
      if (entry.getValue() != null) {
        List<Container> containerList = hostIdToContainers.get(entry.getValue());
        
        if (null == containerList || containerList.isEmpty()) {
          continue;
        }
        
        if ((entry.getValue() != 0) && (containerList.size() == 1)) {
          throw new HamsterException(
              "this shouldn't happen, all container-list with containers == 1 will be returned, host:["
                  + entry.getValue()
                  + "] container count = "
                  + containerList.size());
        }
        
        if (HamsterAppMasterUtils.isLocalHost(entry.getKey()) || (entry.getValue() == 0)) {
          total += containerList.size();
        } else {
          total += containerList.size() - 1;
        }
        
        List<HamsterContainer> hContainers = new ArrayList<HamsterContainer>();
        for (Container c : containerList) {
          hContainers.add(new HamsterContainer(c, resource));
        }
        result.put(entry.getKey(), hContainers);
      }
    }
    
    if (total != n) {
      throw new HamsterException("the final containers in resilt for MPI proc not equals to n, get " + total + " containers");
    }
    
    return result;
  }
  
  void printAskList(int round, List<ResourceRequest> askList) {
    LOG.info("=========== ask list for round :[" + round + "] { ");
    for (ResourceRequest request : askList) {
      LOG.info("    resource request, host:" + request.getHostName() + " container_count:" + request.getNumContainers());
    }
    LOG.info("}");
  }
  
  int makeRemoteRequest(List<ResourceRequest> ask) throws YarnRemoteException, UnknownHostException {
    int nNewSlots = 0; // increased slot number for MPI prob in this allocate
    
    AllocateResponse response = allocator.invokeAllocate(ask); 
    List<Container> allocatedContainers = response.getAMResponse().getAllocatedContainers();
    
    for (Container c : allocatedContainers) {
      String host = HamsterAppMasterUtils.normlizeHostName(c.getNodeId().getHost());
      
      if (!checkIsHostAvailable(host)) {
        // directly release this container
        releaseContainers.add(c.getId());
        continue;
      }
      
      // get container list of this host or create a new list
      List<Container> containerList;
      if (hostToId.containsKey(host)) {
        if (verbose) {
          LOG.info(String.format("insert container to host=%s, id=%d", host, hostToId.get(host)));
        }
        containerList = hostIdToContainers.get(hostToId.get(host));
        if (containerList == null) {
          containerList = new ArrayList<Container>();
          hostIdToContainers.put(hostToId.get(host), containerList);
        }
      } else {
        if (verbose) {
          LOG.info(String.format("insert container to host=%s, id=%d", host, nHosts));
        }
        hostToId.put(host, nHosts);
        containerList = new ArrayList<Container>();
        hostIdToContainers.put(nHosts, containerList);
        nHosts++;
      }
      
      containerList.add(c);
      if (HamsterAppMasterUtils.isLocalHost(host)) {
        nNewSlots++;
      } else if (containerList.size() > 1) {
        nNewSlots++;
      }
    }
    
    return nNewSlots;
  }
  
  // check host is available during allocation
  boolean checkIsHostAvailable(String host) {
    // by default, we think all nodes are available
    return true;
  }
  
  // release containers after each ALLOCATION
  // should be override by child
  int releaseInvalidContainers(List<Container> allocatedContainers) {
    // by default, we do nothing
    return 0;
  }
  
  // should be override by child
  abstract List<ResourceRequest> getAskList();
  
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
  
  // should be override by child
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
        if (verbose) {
          LOG.info("release all containers in host:[" + tmp.hostId
            + "], container count = " + tmp.count);
        }
      } else if (nNeedRelease > 0) {
        if (nNeedRelease >= tmp.count - 1) {
          returnAllContainersInHostId(tmp.hostId);
          nNeedRelease -= (tmp.count - 1);
          if (verbose) {
            LOG.info("release all containers in host:[" + tmp.hostId
              + "], container count = " + tmp.count);
          }
        } else {
          returnPartialContainersInHostId(tmp.hostId, nNeedRelease);
          nNeedRelease = 0;
          if (verbose) {
            LOG.info("release some containers in host:[" + tmp.hostId
              + "], container count = " + nNeedRelease
              + " left-container-count=" + (tmp.count - nNeedRelease));
          }
        }
      }
    }

    if (verbose) {
      LOG.info("STATISTIC: Container Utilization = "
          + (m - releaseContainers.size()) / (float) m);
    }
    
    // check if we need release container in host-0
    if (nNeedRelease > 0) {
      List<Container> containerList = hostIdToContainers.get(0);
      if ((containerList == null) || (containerList.size() <= nNeedRelease)) {
        throw new HamsterException(
            "try to release containers in host-0, but containers in host-0 are not enough to release");
      }
      returnPartialContainersInHostId(0, nNeedRelease);
      if (verbose) {
        LOG.info("release some containers in host:[0], container count = "
            + nNeedRelease + " left-container-count="
            + (containerList.size() - nNeedRelease));
      }
    }
  }
}

package com.pivotal.hamster.appmaster.allocator;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.RackResolver;
import org.junit.Before;
import org.junit.Test;

import com.pivotal.hamster.appmaster.utils.HamsterAppMasterUtils;
import com.pivotal.hamster.common.MockContainer;

public class NaiveStrategyTest {
  static int containerId = 1;
  static String LOCALHOST;
  final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  class Naive_testAllocation1 extends YarnContainerAllocator {
    public Naive_testAllocation1() {
      super(null, null);
    }

    int round = 0;
    
    @Override
    AllocateResponse invokeAllocate(List<ResourceRequest> resourceRequests) {
      try {
        if (round == 0) {
          // check input, 5 * ANY
          checkContains(resourceRequests, "*", 5);

          // mock output, 1 * local + 2 * mock_host1 + 1 * mock_host2
          AllocateResponse response = getEmptyResponse();
          response.getAMResponse().setAllocatedContainers(
              getMockContainers(
                  new String[] { LOCALHOST, "mock_host1", "mock_host2" }, 
                  new int[] { 1, 2, 1 }));
          round++;
          return response;
        } else if (round == 1) {
          // check input, 3 * ANY
          checkContains(resourceRequests, "*", 3);
          
          // mock output, 4 * host2, 3 * host1
          AllocateResponse response = getEmptyResponse();
          response.getAMResponse().setAllocatedContainers(
              getMockContainers(
                  new String[] { "mock_host1", "mock_host2" },
                  new int[] { 3, 3 }));
          round++;
          return response;
        }
      } catch (Exception e) {
        Assert.fail("get exception in invokeAllocate");
      }
      return null;
    }
  }
  
  class Naive_testAllocation2 extends YarnContainerAllocator {
    public Naive_testAllocation2() {
      super(null, null);
    }

    int round = 0;
    
    @Override
    AllocateResponse invokeAllocate(List<ResourceRequest> resourceRequests) {
      try {
        if (round == 0) {
          // check input, 5 * ANY
          checkContains(resourceRequests, "*", 5);

          // mock output, 1 * local + 2 * mock_host1 + 1 * mock_host2
          AllocateResponse response = getEmptyResponse();
          response.getAMResponse().setAllocatedContainers(
              getMockContainers(
                  new String[] { LOCALHOST }, 
                  new int[] { 3 }));
          round++;
          return response;
        } else if (round == 1) {
          // check input, 2 * ANY
          checkContains(resourceRequests, "*", 2);
          
          // mock output, 5 * local
          AllocateResponse response = getEmptyResponse();
          response.getAMResponse().setAllocatedContainers(
              getMockContainers(
                  new String[] { "mock_host1", "mock_host2" },
                  new int[] {  4, 1 }));
          round++;
          return response;
        }
      } catch (Exception e) {
        Assert.fail("get exception in invokeAllocate");
      }
      return null;
    }
  }
  
  @Before
  public void initialize() throws UnknownHostException {
    RackResolver.init(new Configuration());
    LOCALHOST = HamsterAppMasterUtils.getNormalizedLocalhost();
    containerId = 1;
  }
  
  @Test
  public void testAllocation1() {
    /*
     * allocate to 3 nodes, 1 node will be completely return
     */
    
    Naive_testAllocation1 allocator = new Naive_testAllocation1();
    NaiveAllocationStrategy strategy = new NaiveAllocationStrategy(allocator, false);
    ConcurrentLinkedQueue<ContainerId> releaseContainers = new ConcurrentLinkedQueue<ContainerId>();
    strategy.allocate(5, releaseContainers, null, null);
    
    // check release
    // local: 1,
    // host1: 2, 3, 5, 6, 7 
    // host2: [ 4, 8, 9, 10 ]
    checkReleaseQueue(releaseContainers, new int[] { 4, 8, 9, 10 } );
  }
  
  @Test
  public void testAllocation2() {
    /*
     * allocate only to local host, and some containers will be return
     */
    
    Naive_testAllocation2 allocator = new Naive_testAllocation2();
    NaiveAllocationStrategy strategy = new NaiveAllocationStrategy(allocator, false);
    ConcurrentLinkedQueue<ContainerId> releaseContainers = new ConcurrentLinkedQueue<ContainerId>();
    strategy.allocate(5, releaseContainers, null, null);
    
    // check release
    // local: 1, 2, 3
    // host1: 4, 5, 6, [7]
    // host2: [8]
    checkReleaseQueue(releaseContainers, new int[] { 7, 8 } );
  }
  
  AllocateResponse getEmptyResponse() {
    AllocateResponse response = recordFactory.newRecordInstance(AllocateResponse.class);
    AMResponse amResponse = recordFactory.newRecordInstance(AMResponse.class);
    response.setAMResponse(amResponse);
    return response;
  }
  
  static void checkReleaseQueue(Collection<ContainerId> releases, int[] ids) {
    Assert.assertEquals(releases.size(), ids.length);
    Set<Integer> releaseSet = new HashSet<Integer>();
    for (ContainerId id : releases) {
      releaseSet.add(id.getId());
    }
    for (int i = 0; i < ids.length; i++) {
      Assert.assertTrue(releaseSet.contains(ids[i]));
    }
  }
  
  static List<Container> getMockContainers(String[] hosts, int[] slots) {
    List<Container> containers = new ArrayList<Container>();
    Assert.assertEquals(hosts.length, slots.length);
    for (int i = 0; i < hosts.length; i++) {
      for (int j = 0; j < slots[i]; j++) {
        Container c = new MockContainer(containerId, hosts[i]);
        containerId++;
        containers.add(c);
      }
    }
    return containers;
  }
  
  static void checkContains(List<ResourceRequest> requests, String host, int count) {
    boolean find = false;
    for (ResourceRequest req : requests) {
      if (StringUtils.equals(req.getHostName(), host) && (req.getNumContainers() == count)) {
        find = true;
      } else if (StringUtils.equals(req.getHostName(), host) && req.getNumContainers() != count) {
        Assert.fail("find a inrelated request, what happened?");
      }
    }
    Assert.assertTrue(find);
  }
}

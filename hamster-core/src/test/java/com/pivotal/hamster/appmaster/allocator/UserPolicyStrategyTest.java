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

import com.pivotal.hamster.appmaster.common.MockContainer;
import com.pivotal.hamster.appmaster.utils.HamsterAppMasterUtils;
import com.pivotal.hamster.common.HamsterConfig;

public class UserPolicyStrategyTest {
  static int containerId = 1;
  static String LOCALHOST;
  final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  class Allocator_testAllocation1 extends YarnContainerAllocator {
    public Allocator_testAllocation1() {
      super(null, null);
    }

    int round = 0;
    
    @Override
    AllocateResponse invokeAllocate(List<ResourceRequest> resourceRequests) {
      try {
        if (round == 0) {
          // check input
          checkContains(resourceRequests, "mockhost1", 4);
          checkContains(resourceRequests, "mockhost2", 4);
          checkContains(resourceRequests, RackResolver.resolve("mockhost1")
              .getNetworkLocation(), 8);
          checkContains(resourceRequests, "*", 8);

          // mock output, 1 * local + 2 * host1 + 2 * host2
          AllocateResponse response = getEmptyResponse();
          response.getAMResponse().setAllocatedContainers(
              getMockContainers(
                  new String[] { "mockhost1", "mockhost2", LOCALHOST }, 
                  new int[] { 2, 1, 1 }));
          round++;
          return response;
        } else if (round == 1) {
          // check input
          checkContains(resourceRequests, "mockhost1", 2);
          checkContains(resourceRequests, "mockhost2", 3);
          checkContains(resourceRequests, RackResolver.resolve("mockhost1")
              .getNetworkLocation(), 5);
          checkContains(resourceRequests, "*", 5);
          
          // mock output, 2 * host2 +  2 * host1
          AllocateResponse response = getEmptyResponse();
          response.getAMResponse().setAllocatedContainers(
              getMockContainers(
                  new String[] { "mockhost1", "mockhost2" },
                  new int[] { 2, 1 }));
          round++;
          return response;
        } else if (round == 2) {
          // check input
          checkContains(resourceRequests, "mockhost1", 1);
          checkContains(resourceRequests, "mockhost2", 1);
          checkContains(resourceRequests, RackResolver.resolve("mockhost1").getNetworkLocation(), 2);
          checkContains(resourceRequests, "*", 2);
          
          // mock output
          AllocateResponse response = getEmptyResponse();
          response.getAMResponse().setAllocatedContainers(getMockContainers(
              new String[] { "mockhost1", "mockhost2" },
              new int[] {2, 2} ));
          
          round++;
          return response;
        }
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail("get exception in invokeAllocate");
      }
      return null;
    }
  }
  
  class Allocator_testAllocation2 extends YarnContainerAllocator {
    public Allocator_testAllocation2() {
      super(null, null);
    }

    int round = 0;
    
    @Override
    AllocateResponse invokeAllocate(List<ResourceRequest> resourceRequests) {
      try {
        if (round == 0) {
          // check input
          checkContains(resourceRequests, LOCALHOST, 2);
          checkContains(resourceRequests, "mockhost1", 3);
          checkContains(resourceRequests, "mockhost2", 3);
          checkContains(resourceRequests, RackResolver.resolve("mockhost1")
              .getNetworkLocation(), 8);
          checkContains(resourceRequests, "*", 8);

          // mock output, 1 * local + 2 * host1 + 2 * host2
          AllocateResponse response = getEmptyResponse();
          response.getAMResponse().setAllocatedContainers(
              getMockContainers(
                  new String[] { LOCALHOST, "mockhost1", "mockhost2", "mockhostx" }, 
                  new int[] { 1, 1, 1, 1 }));
          round++;
          return response;
        } else if (round == 1) {
          // check input
          checkContains(resourceRequests, LOCALHOST, 1);
          checkContains(resourceRequests, "mockhost1", 2);
          checkContains(resourceRequests, "mockhost2", 2);
          checkContains(resourceRequests, RackResolver.resolve("mockhost1")
              .getNetworkLocation(), 5);
          checkContains(resourceRequests, "*", 5);
          
          // mock output, 2 * host2 +  2 * host1
          AllocateResponse response = getEmptyResponse();
          response.getAMResponse().setAllocatedContainers(
              getMockContainers(
                  new String[] { LOCALHOST, "mockhost1", "mockhosty" },
                  new int[] { 1, 2, 1 }));
          round++;
          return response;
        } else if (round == 2) {
          // check input
          checkContains(resourceRequests, "mockhost2", 1);
          checkContains(resourceRequests, RackResolver.resolve("mockhost1").getNetworkLocation(), 1);
          checkContains(resourceRequests, "*", 1);
          
          // mock output
          AllocateResponse response = getEmptyResponse();
          response.getAMResponse().setAllocatedContainers(getMockContainers(
              new String[] { LOCALHOST, "mockhost1", "mockhostz" },
              new int[] {1, 1, 1} ));
          
          round++;
          return response;
        } else if (round == 3) {
          // check input
          checkContains(resourceRequests, "mockhost2", 1);
          checkContains(resourceRequests, RackResolver.resolve("mockhost1").getNetworkLocation(), 1);
          checkContains(resourceRequests, "*", 1);
          
          // mock output
          AllocateResponse response = getEmptyResponse();
          response.getAMResponse().setAllocatedContainers(getMockContainers(
              new String[] { LOCALHOST, "mockhost1", "mockhost2", "mockhostt" },
              new int[] {2, 2, 3, 2} ));
          
          round++;
          return response;
        }
      } catch (Exception e) {
        e.printStackTrace();
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
    Allocator_testAllocation1 allocator = new Allocator_testAllocation1();
    UserPolicyStrategy strategy = new UserPolicyStrategy(allocator, false);
    ConcurrentLinkedQueue<ContainerId> releaseContainers = new ConcurrentLinkedQueue<ContainerId>();
    
    // we only allow allocate on the two hosts
    Configuration conf = new Configuration();
    conf.set(HamsterConfig.USER_POLICY_HOST_LIST_KEY, "mockhost1,mockhost2");
    strategy.allocate(5, releaseContainers, null, conf);
    
    // check release
    // local: [4],
    // host1: 1, 2, 5, 6, [9, 10]
    // host2: 3, 7, 8, 11, [12]
    checkReleaseQueue(releaseContainers, new int[] { 4, 6, 8, 9 } );
  }
  
  @Test
  public void testAllocation2() {
    Allocator_testAllocation2 allocator = new Allocator_testAllocation2();
    UserPolicyStrategy strategy = new UserPolicyStrategy(allocator, false);
    ConcurrentLinkedQueue<ContainerId> releaseContainers = new ConcurrentLinkedQueue<ContainerId>();
    
    // we only allow allocate on the two hosts
    Configuration conf = new Configuration();
    conf.set(HamsterConfig.USER_POLICY_HOST_LIST_KEY, "mockhost1,mockhost2," + LOCALHOST);
    conf.setInt(HamsterConfig.USER_POLICY_MPROC_KEY, 2);
    strategy.allocate(5, releaseContainers, null, conf);
    
    // check release
    // local: [4],
    // host1: 1, 2, 5, 6, [9, 10]
    // host2: 3, 7, 8, 11, [12]
    checkReleaseQueue(releaseContainers, new int[] { 4, 5, 8, 9, 10, 11, 12, 13, 14, 15, 18, 19, 20 } );  }
  
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


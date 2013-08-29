package com.pivotal.hamster.appmaster.allocator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Assert;
import org.junit.Test;

import com.pivotal.hamster.appmaster.clientserivce.MockClientService;
import com.pivotal.hamster.appmaster.ut.MockDispatcher;
import com.pivotal.hamster.common.HamsterConfig;
import com.pivotal.hamster.common.HamsterContainer;
import com.pivotal.hamster.common.MockContainer;

public class ContainerAllocatorTest {
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  class MockScheduler implements AMRMProtocol {
    private AllocateResponse nextAllocateResponse;
    private Set<Integer> releaseContainers = new HashSet<Integer>();
    private boolean finished = false;
    
    AllocateResponse createEmptyAllocateResponse() {
      AllocateResponse response = recordFactory.newRecordInstance(AllocateResponse.class);
      AMResponse realResponse = recordFactory.newRecordInstance(AMResponse.class);
      response.setAMResponse(realResponse);
      return response;
    }
    
    void setNextAllocateResponse(AllocateResponse response) {
      this.nextAllocateResponse = response;
    }
    
    boolean getFinished() {
      return finished;
    }
    
    public boolean containerReleased(int id) {
      return releaseContainers.contains(id);
    }

    @Override
    public AllocateResponse allocate(AllocateRequest request)
        throws YarnRemoteException {
      List<ContainerId> releaseList = request.getReleaseList();
      if (releaseList != null) {
        for (ContainerId id : releaseList) {
          releaseContainers.add(id.getId());
        }
      }
      
      if (nextAllocateResponse != null) {
        AllocateResponse ret = nextAllocateResponse;
        nextAllocateResponse = null;
        return ret;
      }
      return createEmptyAllocateResponse();
    }

    @Override
    public FinishApplicationMasterResponse finishApplicationMaster(
        FinishApplicationMasterRequest request) throws YarnRemoteException {
      FinishApplicationMasterResponse response = recordFactory.newRecordInstance(FinishApplicationMasterResponse.class);
      finished = true;
      return response;
    }

    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        RegisterApplicationMasterRequest request) throws YarnRemoteException {
      RegisterApplicationMasterResponse response = recordFactory.newRecordInstance(RegisterApplicationMasterResponse.class);
      
      Resource maxResource = recordFactory.newRecordInstance(Resource.class);
      maxResource.setMemory(1024);
      maxResource.setVirtualCores(1);
      
      Resource minResource = recordFactory.newRecordInstance(Resource.class);
      minResource.setMemory(128);
      minResource.setVirtualCores(1);
      
      response.setMaximumResourceCapability(maxResource);
      response.setMinimumResourceCapability(minResource);
      return response;
    }
  }
  
  class ContainerAllocatorUT extends YarnContainerAllocator {
    public ContainerAllocatorUT(Dispatcher dispatcher) {
      super(dispatcher, new MockClientService());
    }

    int containerId = 0;
    
    public AMRMProtocol getScheduler() {
      return scheduler;
    }
    
    @Override
    AMRMProtocol createSchedulerProxy() {
      return new MockScheduler();
    }
    
    @Override
    public Map<String, List<HamsterContainer>> allocate(int n) {
      // do nothing, only set allocate finished
      allocateFinished.getAndSet(true);
      
      // start completed container query thread after allocation finished
      return null;
    }
    
    @Override
    void setApplicationAttemptId() {
      // do nothing
    }
  }
  
  @Test
  public void testContainerAllocator() throws Exception {
    final int TEMP_RM_PULL_INTERVAL = 20; // 20ms 
    MockDispatcher dispatcher = new MockDispatcher();
    
    ContainerAllocatorUT allocator = new ContainerAllocatorUT(dispatcher);
    
    // make a shorter query frequent, we will save some time in UT
    Configuration conf = new Configuration();
    conf.setInt(HamsterConfig.HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME, TEMP_RM_PULL_INTERVAL);
    
    // init and start
    allocator.init(conf);
    allocator.start();
    
    MockScheduler scheduler = (MockScheduler)allocator.getScheduler();
    
    // allocate
    allocator.allocate(5);
    Assert.assertTrue(allocator.allocateFinished.get());
    
    // now we will see if container can be returned
    // we will add some container in AM response, now allocateFinished marked true,
    // so such containers will be released in next query
    AllocateResponse response = recordFactory.newRecordInstance(AllocateResponse.class);
    AMResponse realResponse = recordFactory.newRecordInstance(AMResponse.class);
    List<Container> allocateResponseContainers = new ArrayList<Container>();
    allocateResponseContainers.add(new MockContainer(0));
    allocateResponseContainers.add(new MockContainer(3));
    allocateResponseContainers.add(new MockContainer(5));
    realResponse.setAllocatedContainers(allocateResponseContainers);
    response.setAMResponse(realResponse);
    scheduler.setNextAllocateResponse(response);
    
    // ok, we will sleep 4 * INTERVAL to make sure scheduler received request
    Thread.sleep(4 * TEMP_RM_PULL_INTERVAL);
    Assert.assertTrue(scheduler.containerReleased(0));
    Assert.assertFalse(scheduler.containerReleased(2));
    Assert.assertTrue(scheduler.containerReleased(3));
    Assert.assertTrue(scheduler.containerReleased(5));
    
    // check if finishApplicationMaster works 
    Assert.assertFalse(scheduler.getFinished());
    allocator.stop();
    Assert.assertTrue(scheduler.getFinished());
  }
  
  @Test
  public void testContianerAllocatorTimeout() throws Exception {
    final int TEMP_RM_PULL_INTERVAL = 20; // 20ms 
    final int ALLOCATION_TIMEOUT = 55; // 55 ms
    MockDispatcher dispatcher = new MockDispatcher();
    
    ContainerAllocatorUT allocator = new ContainerAllocatorUT(dispatcher);
    
    // make a shorter query frequent, we will save some time in UT
    Configuration conf = new Configuration();
    conf.setInt(HamsterConfig.HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME, TEMP_RM_PULL_INTERVAL);
    conf.setInt(HamsterConfig.ALLOCATION_TIMEOUT_KEY, ALLOCATION_TIMEOUT);
    
    // init and start
    allocator.init(conf);
    allocator.start();
    
    MockScheduler scheduler = (MockScheduler)allocator.getScheduler();
    
    // now we will see if container can be returned
    // we will add some container in AM response, now allocateFinished marked true,
    // so such containers will be released in next query
    AllocateResponse response = recordFactory.newRecordInstance(AllocateResponse.class);
    AMResponse realResponse = recordFactory.newRecordInstance(AMResponse.class);
    List<Container> allocateResponseContainers = new ArrayList<Container>();
    allocateResponseContainers.add(new MockContainer(0));
    allocateResponseContainers.add(new MockContainer(3));
    allocateResponseContainers.add(new MockContainer(5));
    realResponse.setAllocatedContainers(allocateResponseContainers);
    response.setAMResponse(realResponse);
    scheduler.setNextAllocateResponse(response);
    
    // ok, we will sleep 2 * INTERVAL to see if dispatcher recived failure event
    Thread.sleep(TEMP_RM_PULL_INTERVAL);
    Assert.assertNull(dispatcher.getRecvedEvent());
    Thread.sleep(TEMP_RM_PULL_INTERVAL);
    Assert.assertNull(dispatcher.getRecvedEvent());
    
    // we sleep 2 more interval, now should time out failure triggered
    Thread.sleep(2 * TEMP_RM_PULL_INTERVAL);
    Assert.assertNotNull(dispatcher.getRecvedEvent());
  }
  
  @Test
  public void testContianerAllocatorTimeoutButFinished() throws Exception {
    final int TEMP_RM_PULL_INTERVAL = 20; // 20ms 
    final int ALLOCATION_TIMEOUT = 55; // 55 ms
    MockDispatcher dispatcher = new MockDispatcher();
    
    ContainerAllocatorUT allocator = new ContainerAllocatorUT(dispatcher);
    
    // make a shorter query frequent, we will save some time in UT
    Configuration conf = new Configuration();
    conf.setInt(HamsterConfig.HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME, TEMP_RM_PULL_INTERVAL);
    conf.setInt(HamsterConfig.ALLOCATION_TIMEOUT_KEY, ALLOCATION_TIMEOUT);
    
    // init and start
    allocator.init(conf);
    allocator.start();
    
    MockScheduler scheduler = (MockScheduler)allocator.getScheduler();
    
    // now we will see if container can be returned
    // we will add some container in AM response, now allocateFinished marked true,
    // so such containers will be released in next query
    AllocateResponse response = recordFactory.newRecordInstance(AllocateResponse.class);
    AMResponse realResponse = recordFactory.newRecordInstance(AMResponse.class);
    List<Container> allocateResponseContainers = new ArrayList<Container>();
    allocateResponseContainers.add(new MockContainer(0));
    allocateResponseContainers.add(new MockContainer(3));
    allocateResponseContainers.add(new MockContainer(5));
    realResponse.setAllocatedContainers(allocateResponseContainers);
    response.setAMResponse(realResponse);
    scheduler.setNextAllocateResponse(response);
    
    allocator.allocateFinished.set(true);
    
    // ok, we will sleep 2 * INTERVAL to see if dispatcher recived failure event
    Thread.sleep(TEMP_RM_PULL_INTERVAL);
    Assert.assertNull(dispatcher.getRecvedEvent());
    Thread.sleep(TEMP_RM_PULL_INTERVAL);
    Assert.assertNull(dispatcher.getRecvedEvent());
    
    // we sleep 2 more interval, now should time out failure triggered, but we already set allocateFinished,
    // so this will still not be triggered
    Thread.sleep(2 * TEMP_RM_PULL_INTERVAL);
    Assert.assertNull(dispatcher.getRecvedEvent());
  }
}

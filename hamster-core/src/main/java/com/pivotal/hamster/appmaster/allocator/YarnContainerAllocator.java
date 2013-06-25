package com.pivotal.hamster.appmaster.allocator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.RackResolver;

import com.pivotal.hamster.appmaster.event.HamsterFailureEvent;
import com.pivotal.hamster.appmaster.utils.HadoopRpcUtils;
import com.pivotal.hamster.appmaster.utils.HamsterAppMasterUtils;
import com.pivotal.hamster.common.CompletedContainer;
import com.pivotal.hamster.common.HamsterConfig;
import com.pivotal.hamster.common.HamsterContainer;
import com.pivotal.hamster.common.HamsterException;

public class YarnContainerAllocator extends ContainerAllocator {
  private static final Log LOG = LogFactory.getLog(YarnContainerAllocator.class);
  
  AMRMProtocol scheduler;
  Resource minContainerCapability;
  Resource maxContainerCapability;
  Map<ApplicationAccessType, String> applicationACLs;
  final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  ApplicationAttemptId applicationAttemptId;

  ConcurrentLinkedQueue<ContainerId> releaseContainerQueue;
  ConcurrentLinkedQueue<CompletedContainer> completedContainerQueue;
  Thread queryThread;
  AtomicBoolean stopped;
  boolean registered;
  // is HNP successfully completed
  FinalApplicationStatus hnpStatus;
  int rmPollInterval;
  // we get enough containers to run this job, 
  // containers received after allocation finished will directly released
  AtomicBoolean allocateFinished;
  Dispatcher dispatcher;
  Resource resource;
  Configuration conf;
  
  // allocate can either be used by "allocate resource" or "get completed container"
  // we will not make them used at the same time
  Object allocateLock;
  int responseId;

  public YarnContainerAllocator(Dispatcher dispatcher) {
    super(YarnContainerAllocator.class.getName());
    this.dispatcher = dispatcher;
  }

  @Override
  public Map<String, List<HamsterContainer>> allocate(int n) {
    // implement an algorithm to allocate from RM here, that will fill a Map<ProcessName, ContainerId>
    AllocationStrategy allocateStrategy = getStrategy();
    Map<String, List<HamsterContainer>> result;
    result = allocateStrategy.allocate(n, releaseContainerQueue, resource);
    
    // set allocateFinished
    allocateFinished.getAndSet(true);
    
    // start completed container query thread after allocation finished
    return result;
  }
  
  @Override
  public void completeHnp(FinalApplicationStatus status) {
    this.hnpStatus = status;
  }

  @Override
  public CompletedContainer[] pullCompletedContainers() {
    List<CompletedContainer> completedProcessStatus = new ArrayList<CompletedContainer>();
    while (!completedContainerQueue.isEmpty()) {
      CompletedContainer ps = completedContainerQueue.remove();
      if (ps == null) {
        break;
      }
      completedProcessStatus.add(ps);
    }
    return completedProcessStatus.toArray(new CompletedContainer[0]);
  }
  
  @Override
  public void init(Configuration conf) {
    super.init(conf);
    this.conf = conf;
    scheduler = createSchedulerProxy();
    setApplicationAttemptId();
    releaseContainerQueue = new ConcurrentLinkedQueue<ContainerId>();
    completedContainerQueue = new ConcurrentLinkedQueue<CompletedContainer>();
    stopped = new AtomicBoolean(false);
    registered = false;
    allocateLock = new Object();
    hnpStatus = FinalApplicationStatus.UNDEFINED;
    responseId = 0;
    allocateFinished = new AtomicBoolean(false);
    
    RackResolver.init(conf);
    
    // set rmPollInterval
    rmPollInterval = conf.getInt(HamsterConfig.HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME, 
        HamsterConfig.DEFAULT_HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME);
    
    // read resource request from ENV
    readResourceFromEnv();
              
    LOG.info("init succeed");
  }
  
  @Override
  public void start() {
    // first register to RM
    registerToRM();

    // start completed container query thread
    startCompletedContainerQueryThread();
    
    super.start();

    LOG.info("start succeed");
  }
  
  @Override
  public void stop() {
    queryThread.interrupt();
    try {
      queryThread.join();
    } catch (InterruptedException e) {
      LOG.warn(e);
    }
    if (registered) {
      unregisterFromRM();
    }
    LOG.info("stop succeed");
  }
  
  AllocationStrategy getStrategy() {
    return new ProbabilityBasedAllocationStrategy(this, true);
  }
  
  void readResourceFromEnv() {
    this.resource = recordFactory.newRecordInstance(Resource.class);
    int memory = System.getenv().get("HAMSTER_MEM") == null ? 1024 : Integer
        .parseInt(System.getenv().get("HAMSTER_MEM"));
    int cpu = System.getenv().get("HAMSTER_CPU") == null ? 1 : Integer
        .parseInt(System.getenv().get("HAMSTER_CPU"));
    resource.setMemory(memory);
    resource.setVirtualCores(cpu);
  }
  
  void setApplicationAttemptId() throws HamsterException {
    YarnRPC rpc = HadoopRpcUtils.getYarnRPC(conf);
    applicationAttemptId = HamsterAppMasterUtils.getAppAttemptIdFromEnv();
    if (null == applicationAttemptId) {
      try {
        // here, we will create a YarnClient and submit an application master
        // with
        // UnManaged set, this will be very helpful when we debug
        LOG.info("seems we're in debug mode, no hamster-cli involved");
        InetSocketAddress rmAddress = NetUtils.createSocketAddr(conf
            .get(YarnConfiguration.RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_ADDRESS));
        ClientRMProtocol yarnClient = (ClientRMProtocol) (rpc.getProxy(
            ClientRMProtocol.class, rmAddress, conf));

        // create get new application
        GetNewApplicationRequest newRequest = recordFactory
            .newRecordInstance(GetNewApplicationRequest.class);
        ApplicationId appId = yarnClient.getNewApplication(newRequest)
            .getApplicationId();

        // submit application
        SubmitApplicationRequest submitRequest = recordFactory
            .newRecordInstance(SubmitApplicationRequest.class);
        ApplicationSubmissionContext ctx = recordFactory
            .newRecordInstance(ApplicationSubmissionContext.class);
        ctx.setUnmanagedAM(true);
        ctx.setApplicationId(appId);
        submitRequest.setApplicationSubmissionContext(ctx);
        yarnClient.submitApplication(submitRequest);
      } catch (YarnRemoteException e) {
        LOG.error("get yarn remote exception when trying to use ClientRMProtocol make AM unmanaged", e);
        throw new HamsterException(e);
      }
    }
  }
  
  void startCompletedContainerQueryThread() {
    queryThread = new Thread(new Runnable() {

      @Override
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(rmPollInterval);
            // don't need any resource request, this is a query, do it after allocateFinished
            if (allocateFinished.get()) {
              invokeAllocate(null);
            }
          } catch (Exception e) {
            dispatcher.getEventHandler().handle(new HamsterFailureEvent(e, "exception in allocate"));
            return;
          }
        }
      }
      
    });
    
    queryThread.start();
  }
  
  AllocateResponse invokeAllocate(List<ResourceRequest> resourceRequests) throws YarnRemoteException {
    synchronized(allocateLock) {
      AllocateRequest request = recordFactory.newRecordInstance(AllocateRequest.class);
      request.setApplicationAttemptId(applicationAttemptId);
      request.setResponseId(responseId);
      responseId++;
      
      if (resourceRequests != null) {
        for (ResourceRequest rr : resourceRequests) {
          request.addAsk(rr);
        }
      }
      
      while (!releaseContainerQueue.isEmpty()) {
        ContainerId releaseId;
        try {
          releaseId = releaseContainerQueue.remove();
        } catch (NoSuchElementException e) {
          // just ignore;
          break;
        }
        request.addRelease(releaseId);
      }
      
      // log releases container id
      for (ContainerId id : request.getReleaseList()) {
        LOG.info("release containers to RM, container_id=" + id.getId());
      }
      
      AllocateResponse response = scheduler.allocate(request);
      
      // check if we need directly put releaseId to release table
      if (allocateFinished.get()) {
        List<Container> allocatedContainers = response.getAMResponse().getAllocatedContainers();
        if (allocatedContainers != null) {
          for (Container c : allocatedContainers) {
            releaseContainerQueue.offer(c.getId());
          }
        }
      }

      List<ContainerStatus> completedContainers = response.getAMResponse()
          .getCompletedContainersStatuses();
      for (ContainerStatus status : completedContainers) {
        completedContainerQueue.offer(new CompletedContainer(status
            .getContainerId().getId(), status.getExitStatus()));
      }
      
      return response;
    }
  }
  
  void unregisterFromRM() {
    try {
      FinishApplicationMasterRequest request = recordFactory
          .newRecordInstance(FinishApplicationMasterRequest.class);
      request.setAppAttemptId(applicationAttemptId);
      request.setFinishApplicationStatus(hnpStatus);

      scheduler.finishApplicationMaster(request);
    } catch (Exception e) {
      LOG.error("exception while unregistering:", e);
      throw new YarnException(e);
    }
    registered = false;
  }
  
  void registerToRM() {
    try {
      RegisterApplicationMasterRequest request = recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
      request.setApplicationAttemptId(applicationAttemptId);
      RegisterApplicationMasterResponse response = scheduler.registerApplicationMaster(request);
      minContainerCapability = response.getMinimumResourceCapability();
      maxContainerCapability = response.getMaximumResourceCapability();
      applicationACLs = response.getApplicationACLs();
    } catch (Exception e) {
      LOG.error("exception while registering:", e);
      throw new YarnException(e);
    }
    registered = true;
  }

  AMRMProtocol createSchedulerProxy() {
    final YarnRPC rpc = HadoopRpcUtils.getYarnRPC(conf);
    final InetSocketAddress serviceAddr = conf.getSocketAddr(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnException(e);
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      String tokenURLEncodedStr = System.getenv().get(
          ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME);
      Token<? extends TokenIdentifier> token = new Token<TokenIdentifier>();

      try {
        token.decodeFromUrlString(tokenURLEncodedStr);
      } catch (IOException e) {
        throw new YarnException(e);
      }

      SecurityUtil.setTokenService(token, serviceAddr);
      if (LOG.isDebugEnabled()) {
        LOG.debug("AppMasterToken is " + token);
      }
      currentUser.addToken(token);
    }

    return currentUser.doAs(new PrivilegedAction<AMRMProtocol>() {
      @Override
      public AMRMProtocol run() {
        return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
            serviceAddr, conf);
      }
    });
  }
}

package com.pivotal.hamster.appmaster.allocator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import com.pivotal.hamster.appmaster.HamsterConfig;
import com.pivotal.hamster.appmaster.common.ProcessName;
import com.pivotal.hamster.appmaster.common.ProcessStatus;
import com.pivotal.hamster.appmaster.hnp.HnpLivenessMonitor;
import com.pivotal.hamster.appmaster.utils.HamsterAppMasterUtils;

public class YarnContainerAllocator extends ContainerAllocator {
  private static final Log LOG = LogFactory.getLog(YarnContainerAllocator.class);
  
  private HnpLivenessMonitor mon;
  private Configuration conf;
  private AMRMProtocol scheduler;
  private Resource minContainerCapability;
  private Resource maxContainerCapability;
  private Map<ApplicationAccessType, String> applicationACLs;
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private ApplicationAttemptId applicationAttemptId;
  private Map<ContainerId, ProcessName> procToContainer;
  private ConcurrentLinkedQueue<ContainerId> releaseContainerQueue;
  private ConcurrentLinkedQueue<ProcessStatus> completedContainerQueue;
  private Thread queryThread;
  private AtomicBoolean stopped;
  private boolean registered;
  // is HNP successfully completed
  private FinalApplicationStatus hnpStatus;
  private int rmPollInterval;
  
  // allocate can either be used by "allocate resource" or "get completed container"
  // we will not make them used at the same time
  private Object allocateLock;

  public YarnContainerAllocator(HnpLivenessMonitor mon) {
    super(YarnContainerAllocator.class.getName());
    this.mon = mon;
  }

  @Override
  public Map<ProcessName, Container> allocate(int n) {
    synchronized (allocateLock) {
      // implement an algorithm to allocate from RM here, that will fill a Map<ProcessName, ContainerId>
      return null;
    }
  }
  
  @Override
  public void completeHnp(FinalApplicationStatus status) {
    this.hnpStatus = status;
  }

  @Override
  public ProcessStatus[] pullCompletedContainers() {
    List<ProcessStatus> completedProcessStatus = new ArrayList<ProcessStatus>();
    while (!completedContainerQueue.isEmpty()) {
      ProcessStatus ps = completedContainerQueue.remove();
      if (ps == null) {
        break;
      }
      completedProcessStatus.add(ps);
    }
    return completedProcessStatus.toArray(new ProcessStatus[0]);
  }
  
  @Override
  public void init(Configuration conf) {
    this.conf = conf;
    scheduler = createSchedulerProxy();
    procToContainer = new HashMap<ContainerId, ProcessName>();
    applicationAttemptId = HamsterAppMasterUtils.getAppAttemptIdFromEnv();
    releaseContainerQueue = new ConcurrentLinkedQueue<ContainerId>();
    completedContainerQueue = new ConcurrentLinkedQueue<ProcessStatus>();
    stopped = new AtomicBoolean(false);
    registered = false;
    allocateLock = new Object();
    hnpStatus = FinalApplicationStatus.UNDEFINED;
    
    // set rmPollInterval
    rmPollInterval = conf.getInt(HamsterConfig.HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME, 
        HamsterConfig.DEFAULT_HAMSTER_ALLOCATOR_PULL_INTERVAL_TIME);
      
    super.init(conf);
    
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
  
  void startCompletedContainerQueryThread() {
    queryThread = new Thread(new Runnable() {

      @Override
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(rmPollInterval);
            // don't need any resource request, this is a query
            invokeAllocate(null);
          } catch (YarnException e) {
            LOG.fatal("Error communicating with RM:", e);
            System.exit(1);
          } catch (Exception e) {
            LOG.fatal("Other error:", e);
            System.exit(1);
          }
        }
      }
      
    });
    
    queryThread.start();
  }
  
  void invokeAllocate(List<ResourceRequest> resourceRequest) {
    synchronized(allocateLock) {
      // add code to implement allocate
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
    final Configuration conf = getConfig();
    final YarnRPC rpc = YarnRPC.create(conf);
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

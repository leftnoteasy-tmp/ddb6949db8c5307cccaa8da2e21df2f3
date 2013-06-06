package com.pivotal.hamster.appmaster.launcher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.ProtoUtils;

import com.pivotal.hamster.appmaster.HamsterAppMaster;
import com.pivotal.hamster.appmaster.common.LaunchContext;
import com.pivotal.hamster.appmaster.hnp.HnpLivenessMonitor;

public class YarnContainerLauncher extends ContainerLauncher {
  private static final Log LOG = LogFactory.getLog(YarnContainerLauncher.class);

  private HnpLivenessMonitor mon;
  private YarnRPC rpc;
  private AtomicBoolean failed;
  private AtomicInteger finishedCount;
  
  class YarnContainerLaunchTask implements Callable<Boolean> {
    LaunchContext ctx;
    
    public YarnContainerLaunchTask(LaunchContext ctx) {
      this.ctx = ctx;
    }

    @Override
    public Boolean call() {
      try {
        // create proxy for container manager
        Container container = ctx.getContainer();
        ContainerManager cm = getCMProxy(container.getId(), container
            .getNodeId().getHost() + ":" + container.getNodeId().getPort(),
            container.getContainerToken());
        
        // TODO, fill launch context for cm, set failed to true if got any exception
      } catch (Exception e) {
        LOG.error(e, e);
        failed.getAndSet(true);
      }
      
      // this task is finished
      finishedCount.incrementAndGet();
      return true;
    }
    
  }
  
  public YarnContainerLauncher(HnpLivenessMonitor mon) {
    super(YarnContainerLauncher.class.getName());
    this.mon = mon;
  }
  
  @Override
  public void init(Configuration conf) {
    super.init(conf);
    rpc = YarnRPC.create(conf);
    LOG.info("init succeed");
  }
  
  @Override
  public void start() {
    LOG.info("start succeed");
  }
  
  @Override
  public void stop() {
    LOG.info("stop succeed");
  }

  @Override
  public synchronized boolean[] launch(LaunchContext[] launchContexts) {
    if (null == launchContexts) {
      return null;
    }

    // TODO, make thread count configurable
    ExecutorService executor = Executors.newFixedThreadPool(4);

    // result
    boolean[] results = new boolean[launchContexts.length];

    // init failed and finished count
    failed = new AtomicBoolean(false);
    finishedCount = new AtomicInteger(0);

    Future<Boolean>[] launchResults = new Future[launchContexts.length];
    for (int i = 0; i < launchContexts.length; i++) {
      LaunchContext ctx = launchContexts[i];
      if (ctx == null) {
        LOG.error("one of launch contexts is NULL, please check");
        if (!executor.isShutdown()) {
          executor.shutdown();
        }
        return results;
      }
      launchResults[i] = executor.submit(new YarnContainerLaunchTask(ctx));
    }

    // wait for all tasks finished OR any task failed OR executor terminated
    while (true) {
      if ((failed.get()) || (finishedCount.get() == launchContexts.length)
          || executor.isShutdown()) {
        if (failed.get()) {
          // terminate executor if needed
          if (!executor.isShutdown()) {
            executor.shutdown();
          }
          return results;
        }
        break;
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        return results;
      }
    }

    if (!executor.isShutdown()) {
      executor.shutdown();
    }
    
    // get values from future results
    try {
      for (int i = 0; i < launchContexts.length; i++) {
        Future<Boolean> future = launchResults[i];
        if ((!future.isDone()) || (future.isCancelled())) {
          LOG.error("this should be happen, one of the task not done or cancelled!");
          return results;
        }
        results[i] = future.get();
      }

    } catch (InterruptedException e) {
      LOG.error(e, e);
      return results;
    } catch (ExecutionException e) {
      LOG.error(e, e);
      return results;
    }

    return results;
  }

  protected ContainerManager getCMProxy(ContainerId containerID,
      final String containerManagerBindAddr, ContainerToken containerToken)
      throws IOException {

    final InetSocketAddress cmAddr = NetUtils
        .createSocketAddr(containerManagerBindAddr);
    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    if (UserGroupInformation.isSecurityEnabled()) {
      Token<ContainerTokenIdentifier> token = ProtoUtils
          .convertFromProtoFormat(containerToken, cmAddr);
      // the user in createRemoteUser in this context has to be ContainerID
      user = UserGroupInformation.createRemoteUser(containerID.toString());
      user.addToken(token);
    }

    ContainerManager proxy = user
        .doAs(new PrivilegedAction<ContainerManager>() {
          @Override
          public ContainerManager run() {
            return (ContainerManager) rpc.getProxy(ContainerManager.class,
                cmAddr, getConfig());
          }
        });
    return proxy;
  }
}

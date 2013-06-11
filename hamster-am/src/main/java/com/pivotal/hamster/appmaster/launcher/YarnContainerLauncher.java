package com.pivotal.hamster.appmaster.launcher;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.ProtoUtils;

import com.pivotal.hamster.appmaster.HamsterConfig;
import com.pivotal.hamster.appmaster.common.HamsterException;
import com.pivotal.hamster.appmaster.common.LaunchContext;

public class YarnContainerLauncher extends ContainerLauncher {
  private static final Log LOG = LogFactory.getLog(YarnContainerLauncher.class);

  private YarnRPC rpc;
  AtomicBoolean failed;
  AtomicInteger finishedCount;
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  Map<String, LocalResource> localResources;
  
  class YarnContainerLaunchTask implements Callable<Boolean> {
    LaunchContext ctx;
    
    public YarnContainerLaunchTask(LaunchContext ctx) {
      this.ctx = ctx;
    }

    @Override
    public Boolean call() {
      boolean launchFailed = false;
      try {
        // create proxy for container manager
        Container container = ctx.getContainer();
        ContainerManager cm = getCMProxy(container.getId(), container
            .getNodeId().getHost() + ":" + container.getNodeId().getPort(),
            container.getContainerToken());
        
        // create StartContainerRequest
        StartContainerRequest request = createStartContainerRequest(ctx);
        cm.startContainer(request);
      } catch (Exception e) {
        LOG.error("launch failed", e);
        launchFailed = true;
      }
      
      // this task is finished
      finishedCount.incrementAndGet();
      
      // mark global failed if needed
      if (launchFailed) {
        failed.getAndSet(launchFailed);
      }
      
      return !launchFailed;
    }
    
    StartContainerRequest createStartContainerRequest(LaunchContext ctx) {
      StartContainerRequest request = recordFactory.newRecordInstance(StartContainerRequest.class);
      
      // create and set launch context
      ContainerLaunchContext launchCtx = recordFactory.newRecordInstance(ContainerLaunchContext.class);
      List<String> cmd = new ArrayList<String>();
      
      // set command
      cmd.add(ctx.getArgs());
      launchCtx.setCommands(cmd);
      
      // set container-id
      launchCtx.setContainerId(ctx.getContainer().getId());
      
      // set environment
      launchCtx.setEnvironment(ctx.getEnvars());
      
      // set local resource
      launchCtx.setLocalResources(localResources);
      
      // set resource will be used
      launchCtx.setResource(ctx.getResource());
      
      request.setContainerLaunchContext(launchCtx);
      
      return request;
    }
  }
  
  public YarnContainerLauncher() {
    super(YarnContainerLauncher.class.getName());
  }
  
  @Override
  public void init(Configuration conf) {
    super.init(conf);
    rpc = YarnRPC.create(conf);
    try {
      localResources = loadLocalResources();
    } catch (IOException e) {
      LOG.error("exception when load local resources", e);
      throw new HamsterException(e);
    }
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
        LOG.error(e);
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
  
  static Map<String, LocalResource> loadLocalResources() throws IOException {
    Map<String, LocalResource> resources = new HashMap<String, LocalResource>();
    
    File file = new File(HamsterConfig.HAMSTER_PB_FILE);
    if ((!file.exists()) || (file.isDirectory())) {
      throw new IOException("cannot find a proper file for PB, fil should exist:" + file.getAbsolutePath());
    }
    
    // open file 
    FileInputStream fis = new FileInputStream(file);
    DataInputStream is = new DataInputStream(fis);
    
    // there're n local resource pair
    int n = is.readInt();
    for (int i = 0; i < n; i ++) {
      int len;
      byte[] buffer;
      
      // read key
      len = is.readInt();
      buffer = new byte[len];
      is.readFully(buffer);
      String key = new String(buffer);
      
      // read value
      len = is.readInt();
      buffer = new byte[len];
      is.readFully(buffer);
      LocalResourceProto resProto = LocalResourceProto.parseFrom(buffer);
      LocalResource res = new LocalResourcePBImpl(resProto);
      
      // put it to global resource
      resources.put(key, res);
      
      LOG.debug("add local resource, key=" + key + ", file=" + res.getResource().toString());
    }
    fis.close();
    is.close();
    
    return resources;
  }

  ContainerManager getCMProxy(ContainerId containerID,
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

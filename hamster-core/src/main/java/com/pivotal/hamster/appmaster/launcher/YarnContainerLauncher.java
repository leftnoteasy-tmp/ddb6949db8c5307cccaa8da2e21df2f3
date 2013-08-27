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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.ProtoUtils;

import com.pivotal.hamster.appmaster.utils.HadoopRpcUtils;
import com.pivotal.hamster.appmaster.webapps.HamsterWebAppContext;
import com.pivotal.hamster.common.HamsterConfig;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.common.LaunchContext;

public class YarnContainerLauncher extends ContainerLauncher {
  private static final Log LOG = LogFactory.getLog(YarnContainerLauncher.class);

  AtomicBoolean failed;
  AtomicInteger finishedCount;
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  Map<String, LocalResource> localResources;
  Dispatcher dispatcher;
  Configuration conf;
  
  class YarnContainerLaunchTask implements Callable<Boolean> {
    LaunchContext ctx;
    
    public YarnContainerLaunchTask(LaunchContext ctx) {
      this.ctx = ctx;
    }

    @Override
    public Boolean call() {
      boolean launchFailed = false;
      try {
        LOG.info("do launch for process:" + ctx.getName().toString());

        // create proxy for container manager
        Container container = ctx.getContainer();
        ContainerManager cm = getCMProxy(container.getId(), container
            .getNodeId().getHost() + ":" + container.getNodeId().getPort(),
            container.getContainerToken());
        
        // create StartContainerRequest
        StartContainerRequest request = createStartContainerRequest(ctx);
        LOG.info("before send start container request for " + ctx.getName().toString());
        cm.startContainer(request);
        LOG.info("after send start container request for " + ctx.getName().toString());
        
        LOG.info("after do launch for process:" + ctx.getName().toString());
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
    
    void addEnvs(ContainerLaunchContext ctx) {
      Map<String, String> envs = ctx.getEnvironment();
      if (conf.get(YarnConfiguration.NM_LOCAL_DIRS) != null) {
        envs.put("YARN_NM_LOCAL_DIRS", conf.get(YarnConfiguration.NM_LOCAL_DIRS,
            YarnConfiguration.DEFAULT_NM_LOCAL_DIRS));
      }
    }
    
    StartContainerRequest createStartContainerRequest(LaunchContext ctx) {
      StartContainerRequest request = recordFactory.newRecordInstance(StartContainerRequest.class);
      
      // create and set launch context
      ContainerLaunchContext launchCtx = recordFactory.newRecordInstance(ContainerLaunchContext.class);
      List<String> cmd = new ArrayList<String>();
      
      // set command
      cmd.add(ctx.getArgs());
      launchCtx.setCommands(cmd);
      
      if (conf.get(HamsterConfig.USER_NAME_KEY) == null) {
        throw new HamsterException("user name not set in conf");
      }
      launchCtx.setUser(conf.get(HamsterConfig.USER_NAME_KEY));
      
      // set container-id
      launchCtx.setContainerId(ctx.getContainer().getId());
      
      // set environment
      launchCtx.setEnvironment(ctx.getEnvars());
      addEnvs(launchCtx);
      
      // set local resource
      launchCtx.setLocalResources(localResources);
      
      // set resource will be used
      launchCtx.setResource(ctx.getResource());
      
      request.setContainerLaunchContext(launchCtx);
      
      return request;
    }
  }
  
  public YarnContainerLauncher(Dispatcher dispatcher) {
    super(YarnContainerLauncher.class.getName());
    this.dispatcher = dispatcher;
  }
  
  @Override
  public void init(Configuration conf) {
    super.init(conf);
    this.conf = conf;
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
    long startTime = System.currentTimeMillis();
    
    if (null == launchContexts) {
      return null;
    }

    ExecutorService executor = Executors.newFixedThreadPool(10);

    // result
    boolean[] results = new boolean[launchContexts.length];

    // init failed and finished count
    failed = new AtomicBoolean(false);
    finishedCount = new AtomicInteger(0);
    
    LOG.info("before submit launch tasks");

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
    
    LOG.info("after submit launch tasks");

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
        // add this launched process to web app
        if (results[i]) {
          HamsterWebAppContext.addLaunchedProcess(launchContexts[i]);
        }
      }
    } catch (InterruptedException e) {
      LOG.error(e, e);
      return results;
    } catch (ExecutionException e) {
      LOG.error(e, e);
      return results;
    }
    
    LOG.info("STATISTIC: launch time is :" + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return results;
  }
  
  static Map<String, LocalResource> loadLocalResources() throws IOException {
    Map<String, LocalResource> resources = new HashMap<String, LocalResource>();
    
    File file = new File(HamsterConfig.DEFAULT_LOCALRESOURCE_SERIALIZED_FILENAME);
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
    final YarnRPC rpc = HadoopRpcUtils.getYarnRPC(conf);
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
                cmAddr, conf);
          }
        });
    return proxy;
  }
}

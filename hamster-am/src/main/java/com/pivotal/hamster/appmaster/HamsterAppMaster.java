package com.pivotal.hamster.appmaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.pivotal.hamster.appmaster.allocator.ContainerAllocator;
import com.pivotal.hamster.appmaster.allocator.YarnContainerAllocator;
import com.pivotal.hamster.appmaster.event.HamsterEventHandler;
import com.pivotal.hamster.appmaster.event.HamsterEventType;
import com.pivotal.hamster.appmaster.hnp.DefaultHnpLauncher;
import com.pivotal.hamster.appmaster.hnp.DefaultHnpLivenessMonitor;
import com.pivotal.hamster.appmaster.hnp.DefaultHnpService;
import com.pivotal.hamster.appmaster.hnp.HnpLauncher;
import com.pivotal.hamster.appmaster.hnp.HnpLivenessMonitor;
import com.pivotal.hamster.appmaster.hnp.HnpService;
import com.pivotal.hamster.appmaster.launcher.ContainerLauncher;
import com.pivotal.hamster.appmaster.launcher.YarnContainerLauncher;

public class HamsterAppMaster extends CompositeService {
  private static final Log LOG = LogFactory.getLog(HamsterAppMaster.class);
  
  /**
   * Priority of the HamsterAppMaster shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private HnpLivenessMonitor hnpLivenessMonitor;
  private HnpLauncher hnpLauncher;
  private AsyncDispatcher dispatcher;
  private HnpService hnpService;
  private String[] args;
  
  // The shutdown hook that runs when a signal is received AND during normal
  // close of the JVM.
  static class HamsterAppMasterShutdownHook implements Runnable {
    HamsterAppMaster appMaster;
    HamsterAppMasterShutdownHook(HamsterAppMaster appMaster) {
      this.appMaster = appMaster;
    }
    public void run() {
      LOG.info("HamsterAppMaster received a signal to stop");
      appMaster.stop();
    }
  }

  public HamsterAppMaster(String[] args) {
    super(HamsterAppMaster.class.getName());
    this.args = args;
  }
  
  @Override
  public void init(final Configuration conf) {
    
    // init event dispatcher
    dispatcher = new AsyncDispatcher();
    addService(dispatcher);
    
    // register event handler
    dispatcher.register(HamsterEventType.class, new HamsterEventHandler());
    
    // init liveness monitor
    hnpLivenessMonitor = getHnpLivenessMonitor();
    addService(hnpLivenessMonitor);
    
    // init container allocator
    containerAllocator = getContainerAllocator();
    addService(containerAllocator);
    
    // init container launcher
    containerLauncher = getContainerLauncher();
    addService(containerLauncher);
    
    // init hnp service
    hnpService = getHnpService();
    addService(hnpService);
    
    // init hnp launcher
    hnpLauncher = getHnpLauncher();
    addService(hnpLauncher);
    
    super.init(conf);
  }
  
  HnpService getHnpService() {
    return new DefaultHnpService(dispatcher, containerAllocator, containerLauncher, hnpLivenessMonitor);
  }
  
  HnpLauncher getHnpLauncher() {
    return new DefaultHnpLauncher(dispatcher, hnpService, args);
  }
  
  HnpLivenessMonitor getHnpLivenessMonitor() {
    return new DefaultHnpLivenessMonitor(dispatcher);
  }
  
  ContainerAllocator getContainerAllocator() {
    return new YarnContainerAllocator(dispatcher);
  }
  
  ContainerLauncher getContainerLauncher() {
    return new YarnContainerLauncher(dispatcher);
  }
  
  public static void main(String[] args) {
    try {
      // get conf
      YarnConfiguration conf = new YarnConfiguration();
      
      // create am object
      HamsterAppMaster am = new HamsterAppMaster(args);

      // set am shutdown hook
      ShutdownHookManager.get().addShutdownHook(new HamsterAppMasterShutdownHook(am), SHUTDOWN_HOOK_PRIORITY);
      
      // init am and start
      am.init(conf);
      am.start();
      
      LOG.info("waiting for signal to terminate AM");
      
      // wait for JVM terminate
      while (true) {
        Thread.sleep(60);
      }
    } catch (Throwable t) {
      LOG.fatal("Error starting Hamster App Master", t);
      System.exit(1);
    }
  }

}

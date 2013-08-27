package com.pivotal.hamster.appmaster.launcher;

import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.Test;

import com.pivotal.hamster.appmaster.ut.MockDispatcher;
import com.pivotal.hamster.appmaster.ut.UTUtils;
import com.pivotal.hamster.common.HamsterConfig;
import com.pivotal.hamster.common.LaunchContext;
import com.pivotal.hamster.common.MockContainer;
import com.pivotal.hamster.common.ProcessName;

public class ContainerLauncherTest {
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  
  class MockContainerManager implements ContainerManager {
    boolean failed = false;
    
    public MockContainerManager(boolean failLaunch) {
      failed = failLaunch;
    }
    
    @Override
    public GetContainerStatusResponse getContainerStatus(
        GetContainerStatusRequest arg0) throws YarnRemoteException {
      return null;
    }

    @Override
    public StartContainerResponse startContainer(StartContainerRequest arg0)
        throws YarnRemoteException {
      if (!failed) {
        StartContainerResponse response = recordFactory
            .newRecordInstance(StartContainerResponse.class);
        return response;
      } else {
        throw new YarnException("failed to launchContainer");
      }
    }

    @Override
    public StopContainerResponse stopContainer(StopContainerRequest arg0)
        throws YarnRemoteException {
      return null;
    }
    
    void setNextStartFailed() {
      failed = true;
    }
   
    void unsetNextStartFailed() {
      failed = false;
    }
  }
  
  class ContainerLauncherUT extends YarnContainerLauncher {
    MockContainerManager cm;
  
    public ContainerLauncherUT(Dispatcher dispatcher) {
      super(dispatcher);
      cm = new MockContainerManager(false);
    }
    
    public ContainerLauncherUT(Dispatcher dispatcher, boolean failLaunch) {
      super(dispatcher);
      cm = new MockContainerManager(failLaunch);
    }
    
    @Override 
    ContainerManager getCMProxy(ContainerId containerID,
        final String containerManagerBindAddr, ContainerToken containerToken)
        throws IOException {
      return cm;
    }
    
    void setLocalResources(Map<String, LocalResource> resources) {
      localResources = resources;
    }
  }
  
  @Test
  public void testContainerLauncher() {
    MockDispatcher dispatcher = new MockDispatcher();
    ContainerLauncherUT launcher = new ContainerLauncherUT(dispatcher);
    launcher.conf = new Configuration();
    launcher.conf.set(HamsterConfig.USER_NAME_KEY, "user");
    
    // mock launch context
    LaunchContext[] ctx = new LaunchContext[1024];
    for (int i = 0; i < 1024; i++) {
      MockContainer container = new MockContainer(0);
      LaunchContext lc = new LaunchContext(null, "hello world", "host1", container, new ProcessName(0, 1), UTUtils.GetDefaultResource());
      ctx[i] = lc;
    }
    
    // launch and get result
    boolean[] launchResult = launcher.launch(ctx);
    Assert.assertEquals(1024, launchResult.length);
    
    for (int i = 0; i < 1024; i++) {
      Assert.assertTrue(launchResult[i]);
    }
    
    // launch will be failed
    launcher = new ContainerLauncherUT(dispatcher, true);
    
    // launch and get result
    launchResult = launcher.launch(ctx);
    Assert.assertEquals(1024, launchResult.length);
    
    for (int i = 0; i < 1024; i++) {
      Assert.assertFalse(launchResult[i]);
    }
  }
}

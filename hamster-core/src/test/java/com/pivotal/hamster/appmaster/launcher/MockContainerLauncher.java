package com.pivotal.hamster.appmaster.launcher;

import com.pivotal.hamster.common.LaunchContext;

public class MockContainerLauncher extends ContainerLauncher {
  boolean[] result;
  LaunchContext[] lastRequest;
  
  public MockContainerLauncher() {
    super("mock-launcher");
  }
  
  public void setLaunchResult(boolean[] result) {
    this.result = result;
  }
  
  public LaunchContext[] getLastRequest() {
    return lastRequest;
  }

  @Override
  public boolean[] launch(LaunchContext[] request) {
    lastRequest = request;
    if (null == result) {
      return new boolean[request.length];
    }
    return result;
  }

}

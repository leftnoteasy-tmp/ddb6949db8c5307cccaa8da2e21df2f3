package com.pivotal.hamster.appmaster.hnp;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.junit.Test;

import com.pivotal.hamster.appmaster.event.HamsterEventType;
import com.pivotal.hamster.appmaster.ut.MockDispatcher;

public class HnpLauncherTest {
  static class HnpLauncherUT extends DefaultHnpLauncher {

    public HnpLauncherUT(Dispatcher dispatcher, String[] args) {
      super(dispatcher, args);
    }
    
  }
  
  @Test
  public void testLaunchHnp() throws Exception {
    MockDispatcher dispatcher = new MockDispatcher();
    HnpLauncherUT launcher = new HnpLauncherUT(dispatcher, new String[] { "sleep", "1"});
    launcher.init(new Configuration());
    launcher.start();
    launcher.execThread.join();
    Assert.assertNotNull(dispatcher.getRecvedEvent());
    Assert.assertEquals(HamsterEventType.FAILURE, dispatcher.getRecvedEvent().getType());
  }
}

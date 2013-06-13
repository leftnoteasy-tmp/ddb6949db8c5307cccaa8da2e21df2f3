package com.pivotal.hamster.appmaster.ut;

import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;

import com.pivotal.hamster.appmaster.event.HamsterEvent;

public class MockDispatcher implements Dispatcher{
  private MockHamsterEventHandler handler;
  
  static class MockHamsterEventHandler implements EventHandler<HamsterEvent> {
    private HamsterEvent event = null; 
    
    @Override
    public synchronized void handle(HamsterEvent event) {
      if (this.event == null) {
        this.event = event;
      }
    }
    
    public HamsterEvent getRecvedEvent() {
      return event;
    }
  }
  
  @Override
  public EventHandler<HamsterEvent> getEventHandler() {
    return handler;
  }
  
  public MockDispatcher() {
    handler = new MockHamsterEventHandler();
  }

  @Override
  public void register(Class<? extends Enum> arg0, EventHandler arg1) {
    // do nothing
  }
  
  public HamsterEvent getRecvedEvent() {
    return handler.getRecvedEvent();
  }
}

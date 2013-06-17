package com.pivotal.hamster.appmaster.event;

public class HamsterFailureEvent extends HamsterEvent {
  Throwable exception;
  String diagnostics; 

  public HamsterFailureEvent(Throwable exception, String diagnostics) {
    super(HamsterEventType.FAILURE);
    this.exception = exception;
    this.diagnostics = diagnostics;
  }

  public Throwable getThrowable() {
    return exception;
  }
  
  public String getDiagnostics() {
    return diagnostics;
  }
}

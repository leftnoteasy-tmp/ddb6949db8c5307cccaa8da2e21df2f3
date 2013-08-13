package com.pivotal.hamster.common;

public class HamsterCliParseException extends HamsterException {
  
  private static final long serialVersionUID = 3053552899116302974L;
  
  public HamsterCliParseException(Throwable t) {
    super(t);
  }
  
  public HamsterCliParseException(String message) {
    super(message);
  }

}

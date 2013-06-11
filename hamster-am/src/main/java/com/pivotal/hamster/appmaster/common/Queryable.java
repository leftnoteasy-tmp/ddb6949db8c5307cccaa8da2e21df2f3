package com.pivotal.hamster.appmaster.common;

public interface Queryable {
  public boolean isFailed();
  public String getDiagnostics();
}

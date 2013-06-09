package com.pivotal.hamster.appmaster.common;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;

public class LaunchContext {
  Map<String, String> envars;
  String args;
  String host;
  Container container;
  ProcessName name;
  
  public LaunchContext(Map<String, String> envars, String args, String host, Container container, ProcessName name) {
    this.envars = envars;
    this.args = args;
    this.host = host;
    this.container = container;
    this.name = name;
  }
  
  public Map<String, String> getEnvars() {
    return envars;
  }
  
  public String getArgs() {
    return args;
  }
  
  public String getHost() {
    return host;
  }
  
  public Container getContainer() {
    return container;
  }

  public ProcessName getName() {
    return name;
  }
}

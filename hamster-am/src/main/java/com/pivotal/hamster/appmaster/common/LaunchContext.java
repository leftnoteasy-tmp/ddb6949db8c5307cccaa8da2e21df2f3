package com.pivotal.hamster.appmaster.common;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class LaunchContext {
  Map<String, String> envars;
  String args;
  String host;
  Container container;
  
  public LaunchContext(Map<String, String> envars, String args, String host, Container container) {
    this.envars = envars;
    this.args = args;
    this.host = host;
    this.container = container;
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
}

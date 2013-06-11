package com.pivotal.hamster.appmaster.common;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;

public class LaunchContext {
  Map<String, String> envars;
  String args;
  String host;
  Container container;
  ProcessName name;
  Resource resource;
  
  public LaunchContext(Map<String, String> envars, String args, String host,
      Container container, ProcessName name, Resource resource) {
    this.envars = envars;
    this.args = args;
    this.host = host;
    this.container = container;
    this.name = name;
    this.resource = resource;
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
  
  public Resource getResource() {
    return resource;
  }
}

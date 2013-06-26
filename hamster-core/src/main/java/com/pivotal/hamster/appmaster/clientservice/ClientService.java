package com.pivotal.hamster.appmaster.clientservice;

import org.apache.hadoop.yarn.service.AbstractService;

abstract public class ClientService extends AbstractService {
  public ClientService(String name) {
    super(name);
  }
  
  abstract public int getHttpPort();
}

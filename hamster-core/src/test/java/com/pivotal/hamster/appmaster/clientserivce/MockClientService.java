package com.pivotal.hamster.appmaster.clientserivce;

import com.pivotal.hamster.appmaster.clientservice.ClientService;

public class MockClientService extends ClientService {

  public MockClientService() {
    super(MockClientService.class.getName());
  }

  @Override
  public int getHttpPort() {
    return 0;
  }

}

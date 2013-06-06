package com.pivotal.hamster.appmaster.hnp;

import org.apache.hadoop.yarn.service.AbstractService;

abstract public class HnpService extends AbstractService {

  public HnpService(String name) {
    super(HnpService.class.getName());
  }

}

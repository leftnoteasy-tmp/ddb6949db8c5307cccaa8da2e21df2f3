package com.pivotal.hamster.appmaster.webapps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.Controller;

import com.google.inject.Inject;

public class HamsterAppController extends Controller {

  private final Configuration conf;
  private final HamsterApp app;

  @Inject
  public HamsterAppController(HamsterApp app, Configuration conf, RequestContext ctx) {
    super(ctx); 
    this.conf = conf;
    this.app = app;
  }

  @Override
  public void index() {
    set(HamsterMPIWebConst.APP_ID, "TO BE ADDED");
    render(IndexPage.class);
  }

}

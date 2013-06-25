package com.pivotal.hamster.appmaster.webapps;

import org.apache.hadoop.yarn.webapp.Controller;

import com.google.inject.Inject;

public class HamsterAppController extends Controller {
  @Inject
  public HamsterAppController(RequestContext ctx) {
    super(ctx);
  }

  @Override
  public void index() {
    render(IndexPage.class);
  }
}

package com.pivotal.hamster.appmaster.clientservice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

import com.pivotal.hamster.appmaster.webapps.AMWebApp;

public class ClientServiceImpl extends ClientService{
  private static final Log LOG = LogFactory.getLog(ClientService.class);

  WebApp wa;
  Configuration conf;
  
  public ClientServiceImpl() {
    super(ClientServiceImpl.class.getName());
  }
  
  public void init(Configuration conf) {
    this.conf = conf;
    super.init(conf);
  }
  
  @Override
  public void start() {
    // start web app
    try {
      wa = WebApps.$for("yarn", null, null, null).with(conf)
          .start(new AMWebApp());
    } catch (Exception e) {
      LOG.error("OOPs! failed to start web app", e);
    }
  }

  @Override
  public int getHttpPort() {
    return wa.port();
  }
}

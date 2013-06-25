/**
 * 
 */
package com.pivotal.hamster.appmaster.webapps;

import org.apache.hadoop.yarn.webapp.WebApp;

public class AMWebApp extends WebApp {

  @Override
  public void setup() {
    route("/", HamsterAppController.class);
    route("/hamster", HamsterAppController.class);
  }
}

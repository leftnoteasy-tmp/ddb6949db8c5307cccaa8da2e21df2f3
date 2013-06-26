/**
 * 
 */
package com.pivotal.hamster.appmaster.webapps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;

public class AMWebApp extends WebApp {
  private static final Log LOG = LogFactory.getLog(AMWebApp.class);

  @Override
  public void setup() {
    route("/", HamsterAppController.class);
    route("/hamster", HamsterAppController.class);
  }

  /*
   * for test purpose only
   */
  public static void main(String[] args) {
    WebApp wa = null;
    wa = WebApps.$for("yarn", null, null, null).with(new Configuration())
        .start(new AMWebApp());
    LOG.info("web page located at: http://localhost:" + wa.port());
    wa.joinThread();
  }
}

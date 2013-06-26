package com.pivotal.hamster.appmaster.webapps;

import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

public class NavBlock extends HtmlBlock {
  @Override 
  protected void render(Block html) {
    html.
      div("#nav").
        h3("Tools").
        ul().
          li().a("/conf", "Configuration")._().
          li().a("/stacks", "Thread dump")._().
          li().a("/logs", "Logs")._().
          li().a("/metrics", "Metrics")._()._()._();
  }
}

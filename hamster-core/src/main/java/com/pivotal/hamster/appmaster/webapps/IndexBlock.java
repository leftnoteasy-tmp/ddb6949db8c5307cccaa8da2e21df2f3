package com.pivotal.hamster.appmaster.webapps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

public class IndexBlock extends HtmlBlock {
  private static final Log LOG = LogFactory.getLog(IndexBlock.class);

  @Override
  protected void render(Block html) {
    html.h2()._("Application Id :" + $(HamsterMPIWebConst.APP_ID))._();
    html.div()._("By Pivotal Initiative")._();
  }
}
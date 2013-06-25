package com.pivotal.hamster.appmaster.webapps;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.pivotal.hamster.appmaster.utils.HamsterAppMasterUtils;
import com.pivotal.hamster.common.LaunchContext;

public class IndexBlock extends HtmlBlock {
  private static final Log LOG = LogFactory.getLog(IndexBlock.class);
  private String localNodeAddr;

  @Override
  protected void render(Block html) {
    LaunchContext[] launchedContexts = HamsterWebAppContext.getSortedLaunchContexts();
    int mpiJobStartIdx = -1;
    for (int i = 0; i< launchedContexts.length; i++) {
      if (launchedContexts[i].getName().getJobId() > 0) {
        mpiJobStartIdx = i;
        break;
      }
    }
    
    String userName = null;
    try {
      userName = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.warn("error while geting userName", e);
      userName = "null-user-name";
    }
    
    // make mpi proc table
    TBODY<TABLE<Hamlet>> tbody = html.
        h2("Launched MPI procs").
        table("#jobs").
        thead().
        tr().
        th("left", "Rank").
        th("left", "Host").
        th("left", "Log URL")._()._().
        tbody();
    
    if (mpiJobStartIdx >= 0) {
      for (int i = mpiJobStartIdx; i < launchedContexts.length; i++) {
        LaunchContext ctx = launchedContexts[i];
        tbody.tr().
          td(String.valueOf(ctx.getName().getVpId())).
          td(ctx.getHost()).
          td().a(String.format("http://%s/node/containerlogs/%s/" + userName,
              ctx.getContainer().getNodeHttpAddress(),
              ctx.getContainer().getId().toString()),
              String.format("container:%s log link", ctx.getContainer().getId().toString()))._()._();
      }
    }
    
    // render mpi proc table
    tbody._()._();

    // make mpi daemon table
    tbody = html.
        h2("Launched Daemon procs").
        table("#jobs").
        thead().
        tr().
        th("left", "Rank").
        th("left", "Host").
        th("left", "Log URL")._()._().
        tbody();
    
    int daemonEndIdx = mpiJobStartIdx;
    if (daemonEndIdx < 0) {
      daemonEndIdx = launchedContexts.length;
    }
    
    // add hnp
    String localNMAddr = HamsterAppMasterUtils.getLocalNMHttpAddr();
    ContainerId localContainerId = HamsterAppMasterUtils.getContainerIdFromEnv();
    
    if (localNMAddr != null && localContainerId != null) {
      tbody.tr().
        td("HNP - 0").
        td(System.getenv("NM_HOST")).
        td().a(String.format("http://%s/node/containerlogs/%s/" + userName,
            localNMAddr,
            localContainerId.toString()),
            String.format("container:%s log link", localContainerId.toString()))._()._();
    }
    
    if (mpiJobStartIdx >= 0) {
      for (int i = 0; i < daemonEndIdx; i++) {
        LaunchContext ctx = launchedContexts[i];
        tbody.tr().
          td(String.valueOf(ctx.getName().getVpId())).
          td(ctx.getHost()).
          td().a(String.format("http://%s/node/containerlogs/%s/" + userName,
              ctx.getContainer().getNodeHttpAddress(),
              ctx.getContainer().getId().toString()),
              String.format("container:%s log link", ctx.getContainer().getId().toString()))._()._();
      }
    }
    
    // make mpi daemon table
    tbody._()._();
  }
}
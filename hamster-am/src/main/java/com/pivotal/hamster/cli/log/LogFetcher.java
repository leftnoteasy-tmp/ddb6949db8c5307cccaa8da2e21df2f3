package com.pivotal.hamster.cli.log;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.pivotal.hamster.common.HamsterConfig;

public class LogFetcher {
  private static final Log LOG = LogFactory.getLog(LogFetcher.class);

  // which file will be read, like stdout, stderr, syslog, etc.
  Set<String> enabledKeys;
  
  // app id string of this job
  ApplicationId appId;
  
  Configuration conf;
  FileSystem fs;
  
  // user name in cluster
  String user;
  
  int totalNp;
  
  public LogFetcher(boolean succeed, ApplicationId appId, Configuration conf, String user, FileSystem fs, int totalNp) {
    this.appId = appId;
    this.conf = conf;
    this.user = user;
    this.fs = fs;
    this.totalNp = totalNp;
    this.enabledKeys = getWhichLogFileShouldBeRead(succeed);
  }
  
  public boolean checkLogFetchable() throws IOException {
    boolean aggEnable = this.conf.getBoolean("yarn.log-aggregation-enable", false);
    if (!aggEnable) {
      LOG.warn("yarn.log-aggregation-enable is not enabled, cannot aggregate logs, please go to log directory check log");
      return false;
    }
    return true;
  }
  
  public void readAll(FinalApplicationStatus amStatus) throws IOException {
    Path remoteRootLogDir = new Path(this.conf.get(
        YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    Path appLogDir = LogAggregationUtils.getRemoteAppLogDir(remoteRootLogDir, appId, user, 
        LogAggregationUtils.getRemoteNodeLogDirSuffix(this.conf));
    
    // get container tree
    ContainerTreeNode root = getContainerTree(appLogDir);
    LOG.info("generated container structure from log-dir:" + appLogDir);
    root.printTree();
    
    // print container logs by different conditions
    traverseAndPrint(root, amStatus);
  }
  
  private Set<String> getWhichLogFileShouldBeRead(boolean succeed) {
    // first see if user set the HAMSTER_ENABLED_LOGKEYS_KEY
    String keys = conf.get(HamsterConfig.HAMSTER_ENABLED_LOGKEYS_KEY);
    Set<String> ret = new HashSet<String>();
    
    if (keys == null || keys.isEmpty()) {
      /* we will use default strategy:
       * 1) if state is finished, only stdout
       * 2) otherwise, stdout + stderr + syslog
       */
      if (succeed) {
        ret.add("stdout");
        return ret;
      }
      
      // must be failed
      ret.add("stderr");
      ret.add("stdout");
      ret.add("syslog");
      return ret;
    }
    
    // use specified something, add them to set
    for (String s : keys.split(",")) {
      s = s.trim();
      if (s.isEmpty()) {
        continue;
      }
      ret.add(s);
    }
    
    return ret;
  }
  
  private ContainerTreeNode createContainerTreeNode(ContainerTreeNode father, String containerIdStr, Path logFile) {
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    ContainerTreeNode.ContainerType type = ContainerTreeNode.ContainerType.APP;
     
    if (containerId.getId() == 1) {
      // first container will always mpirun
      type = ContainerTreeNode.ContainerType.HNP;
    } else if (father.getChildrenNum() == 0) {
      // first container of this node should be daemon
      type = ContainerTreeNode.ContainerType.DAEMON;
    }
    
    return new ContainerTreeNode(type, containerIdStr, logFile, conf);
  }
  
  private List<String> getSortedContainers(Path path) throws IOException {
    // parse this sequence file
    LogReader logReader = new LogReader(conf, path);
    LogKey key = new LogKey();
    DataInputStream valueStream = logReader.next(key);
    List<String> ret = new ArrayList<String>();
    
    while (valueStream != null) {
      String containerIdStr = key.toString();
      valueStream.close();
      
      ret.add(containerIdStr);
      
      // get next container in this node
      valueStream = logReader.next(key);
    }
    logReader.close();
    Collections.sort(ret);
    
    return ret;
  }
  
  private ContainerTreeNode getContainerTree(Path appLogDir) throws IOException {
    Set<String> processedFiles = new HashSet<String>();
    // finished container number for MPI procs
    int finishedContainerNum = 0; 
    ContainerTreeNode root = new ContainerTreeNode(ContainerTreeNode.ContainerType.ROOT, "yarn-cluster", null, conf);
    // how many turns that we haven't get new finished containers
    int noContainerFetchedTurns = 0;
    
    while (finishedContainerNum < totalNp + 1) {
      FileStatus[] statuses = fs.listStatus(appLogDir);
      boolean containerFetched = false;
      for (FileStatus status : statuses) {
        Path path = status.getPath();
        
        // we will process finished and un-processed files
        if ((!path.getName().endsWith(".tmp")) 
            && (!processedFiles.contains(path.toString()))) {
          processedFiles.add(path.toString());
          
          // insanity check, shouldn't happen
          if (!fs.isFile(path)) {
            LOG.warn("something in app log dir but not a valid file, logFile:" + path.toString());
            continue;
          }
          
          // create a TreeNode for this node (node name is filename of this logfile)
          ContainerTreeNode node = new ContainerTreeNode(path.getName(), conf);
          ContainerTreeNode daemonNode = null;
          
          // get sorted container-id-str list
          List<String> containerIdList = getSortedContainers(path);
          for (String containerIdStr : containerIdList) {
            ContainerTreeNode containerNode = createContainerTreeNode(node, containerIdStr, path);
            if (daemonNode == null) {
              daemonNode = containerNode;
            }
            node.addChild(containerNode);
          }
          
          // add node to root
          root.addChild(node);
          
          if (daemonNode.type == ContainerTreeNode.ContainerType.HNP) {
            finishedContainerNum += node.getChildrenNum();
          } else {
            if (node.getChildrenNum() > 0) {
              finishedContainerNum += node.getChildrenNum() - 1;
            }
          }
          
          containerFetched = true;
        }
      }
      
      // check if we will continue fetch
      if (!containerFetched) {
        noContainerFetchedTurns++;
      } else {
        noContainerFetchedTurns = 0;
      }
      int userWaitTime = conf.getInt(
          HamsterConfig.HAMSTER_LOG_AGGREGATION_WAIT_TIME,
          HamsterConfig.DEFAULT_HAMSTER_LOG_WAIT_TIME);
      int maxWaitTurns = userWaitTime / 100;
      if (maxWaitTurns <= 0) {
        maxWaitTurns = 1;
      }
      
      // wait for a moment and try next round
      if (maxWaitTurns <= noContainerFetchedTurns) {
        LOG.error("not all container log fetched");
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
    
    return root;
  }
  
  private void traverseAndPrint(ContainerTreeNode node, FinalApplicationStatus amStatus) throws IOException {
    node.printCurrentLog(enabledKeys, amStatus);
    
    if (node.getChildrenNum() > 0) {
      for (ContainerTreeNode child : node.children) {
        traverseAndPrint(child, amStatus);
      }
    }
  }
  
  /**
   * for test purpose only
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    Path appLogPath = new Path(args[0]);
    int np = Integer.parseInt(args[1]);
    FinalApplicationStatus status = FinalApplicationStatus.valueOf(args[2]);
    
    Set<String> enableKeys = new HashSet<String>();
    enableKeys.add("stdout");
    // enableKeys.add("stderr");
    LogFetcher fetcher = new LogFetcher(true, null, new Configuration(), null, FileSystem.getLocal(new Configuration()), np);
    
    // get container tree
    ContainerTreeNode root = fetcher.getContainerTree(appLogPath);
    LOG.info("container structure:");
    root.printTree();
    
    // print container logs by different conditions
    fetcher.traverseAndPrint(root, status);
  }
}
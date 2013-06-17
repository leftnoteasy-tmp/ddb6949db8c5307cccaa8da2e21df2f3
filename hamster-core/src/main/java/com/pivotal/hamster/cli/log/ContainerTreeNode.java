package com.pivotal.hamster.cli.log;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.ContainerLogsReader;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader;

public class ContainerTreeNode {
  private static final Log LOG = LogFactory.getLog(ContainerTreeNode.class);

  public enum ContainerType {
    ROOT,       // root node, not a actual container
    INTERNAL,   // internal node, not a actual container
    HNP,        // mpirun container
    DAEMON,     // orted container
    APP         // mpi proc container
  }

  final List<ContainerTreeNode> children;
  final String printName;
  final String name;
  final ContainerType type;
  final Path logFile;
  final Configuration conf;

  public ContainerTreeNode(ContainerType type, String name, Path logFile, Configuration conf) {
    this.name = name;
    if (type != ContainerType.INTERNAL && type != ContainerType.ROOT) {
      this.printName = "(" + type.name() + ")" + name;
    } else {
      this.printName = name;
    }
    this.type = type;
    this.children = new ArrayList<ContainerTreeNode>();
    this.logFile = logFile;
    this.conf = conf;
  }
  
  public ContainerTreeNode(String name, Configuration conf) {
    this(ContainerType.INTERNAL, name, null, conf);
  }

  public void addChild(ContainerTreeNode child) {
    children.add(child);
  }
  
  public int getChildrenNum() {
    return children.size();
  }

  public void printTree() {
    printTree("", true);
  }
  
  public void printCurrentLog(Set<String> enabledKeys, FinalApplicationStatus amStatus) throws IOException {
    if (type == ContainerType.ROOT) {
      // skip root
    } else if (type == ContainerType.INTERNAL) {
      LOG.info("read log in node:" + printName);
    } else if (type == ContainerType.HNP || type == ContainerType.DAEMON) {
      if (amStatus == FinalApplicationStatus.SUCCEEDED) {
        // we will not provide daemon log to users when application is succeed
        return;
      }
      realPrintLog(enabledKeys, amStatus);
    } else {
      realPrintLog(enabledKeys, amStatus);
    }
  }
  
  private void printOut(String s, String logType) {
    if (StringUtils.equals(logType, "stdout")) {
      System.out.print(s);
      return;
    }
    System.err.print(s);
  }
  
  private void printlnOut(String s, String logType) {
    if (StringUtils.equals(logType, "stdout")) {
      System.out.println(s);
      return;
    }
    System.err.println(s);
  }
  
  private void realPrintLog(Set<String> enabledKeys, FinalApplicationStatus amStatus) throws IOException {
    LogKey key = new LogKey();
    LogReader logReader = new LogReader(conf, logFile);
    DataInputStream valueStream = logReader.next(key);
    while (valueStream != null) {
      // check if the container with same containerStr of our own
      if (!StringUtils.equalsIgnoreCase(key.toString(), name)) {
        valueStream.close();
        valueStream = logReader.next(key);
        continue;
      }
      
      // start print
      System.err.println();
      LOG.info("< read container:" + key.toString() + " >");
      ContainerLogsReader containerLogReader = new ContainerLogsReader(valueStream);
      String logType = containerLogReader.nextLog();
      while (logType != null) {
        byte[] buffer = new byte[1024];
        int readLen = -1;
        if ((enabledKeys == null) || (enabledKeys.contains(logType))) {
          printlnOut("", logType);
          printlnOut("< read log name:" + logType + " >", logType);
          while ((readLen = containerLogReader.read(buffer, 0, buffer.length)) >= 0) {
            printOut(new String(buffer, 0, readLen), logType);
          }
          printlnOut("", logType);
          printlnOut("< finished read log >", logType);
        }
        logType = containerLogReader.nextLog();
      }
      valueStream.close();
      valueStream = logReader.next(key);
    }
    logReader.close();
  }

  private void printTree(String prefix, boolean isTail) {
    System.err.println(prefix + (isTail ? "└── " : "├── ") + printName);
    for (Iterator<ContainerTreeNode> iterator = children.iterator(); iterator
        .hasNext();) {
      iterator.next().printTree(prefix + (isTail ? "    " : "│   "),
          !iterator.hasNext());
    }
  }
}

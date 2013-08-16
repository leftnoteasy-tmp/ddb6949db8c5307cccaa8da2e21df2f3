package com.pivotal.hamster.cli_new.processor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.cli_new.utils.CliUtils;
import com.pivotal.hamster.common.HamsterCliParseException;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.common.HostExprParser;
import com.pivotal.hamster.commons.cli.Option;

public class HostProcessor implements CliProcessor {
  private static final Log LOG = LogFactory.getLog(HostProcessor.class);

  String readHostFile(String filename) {
    try {
      File file = new File(filename);
      if (!file.exists() || file.isDirectory()) {
        throw new IOException("failed to read hostfile:" + filename);
      }
      FileReader fr = new FileReader(file);
      BufferedReader br = new BufferedReader(fr);
      String line;
      String hostlist = "";
      while (null != (line = br.readLine())) {
        // simply check for comments or blank line
        if (line.startsWith("#") || line.trim().isEmpty()) {
          continue;
        }
        if (!hostlist.isEmpty()) {
          hostlist = hostlist + ",";
        }
        
        // we need handle case like ^www.example.com slots=xx maxslots=yy$
        // so we will simply remove contents after " "
        if (line.indexOf(' ') >= 0) {
          line = line.substring(0, line.indexOf(' '));
        }
        hostlist = hostlist + line;
      }
      fr.close();
      br.close();
      return hostlist;
    } catch (IOException e) {
      throw new HamsterCliParseException(e);
    }
  }
  
  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    boolean useDefaultHost = false;
    List<Option> newOptions = CliUtils.removeOptions(new String[] { "host",
        "H", "hostfile", "machinefile", "default-hostfile" }, options);

    // first we need to check if -default-host specified and -host or -hostlist not specified
    if (CliUtils.containsOption("default-hostfile", options)) {
      useDefaultHost = true;
      if (CliUtils.containsOption("host", options)
          || CliUtils.containsOption("H", options)
          || CliUtils.containsOption("hostfile", options)
          || CliUtils.containsOption("machinefile", options)) {
        // we need ignore the default-hostfile because host specified
        useDefaultHost = false;
      }
    }
    
    if (useDefaultHost) {
      Option defaultHostfileOption = CliUtils.getOption("default-hostfile", options);
      String hosts = readHostFile(defaultHostfileOption.getValue());
      context.setHosts(hosts);
      CliUtils.replaceExistingOptions(options, newOptions);
      return ProcessResultType.SUCCEED;
    }
    
    String hostfile = null;
    
    // we need check if -host/-H and hostfile/machine file specified at the same time
    if ((CliUtils.containsOption("host", options) || CliUtils.containsOption("H", options))
        && (CliUtils.containsOption("hostfile", options) || CliUtils.containsOption("machinefile", options))) {
      LOG.error("you specified host and hostfile at the same time, please check.");
      throw new HamsterCliParseException("you specified host and hostfile at the same time, please check.");
    }
    
    // we need check if multiple hostfile/machine file specified
    int hostfileCount = 0;
    for (Option op : options) {
      if (StringUtils.equals(op.getOpt(), "hostfile") || (StringUtils.equals(op.getOpt(), "machinefile"))) {
        hostfile = op.getValue();
        hostfileCount++;
      }
    }
    if (hostfileCount > 1) {
      LOG.error("more than one hostfile specified, which is not supported");
      throw new HamsterCliParseException("more than one hostfile specified, which is not supported");
    }
    
    if (hostfileCount == 0) {
      // check if we need parse host
      int hostCount = 0;
      String hostExpr = null;
      for (Option op : options) {
        if (StringUtils.equals(op.getOpt(), "H") || StringUtils.equals(op.getOpt(), "host")) {
          hostCount++;
          hostExpr = op.getValue();
        }
      }
      
      if (hostCount > 1) {
        LOG.error("more than one -host specified, which is not supported");
        throw new HamsterCliParseException("more than one -host specified, which is not supported");
      }
      
      // use our tool to expand the expression
      String hosts = HostExprParser.parse(hostExpr);
      context.setHosts(hosts);
    } else {
      String hosts = readHostFile(hostfile);
      context.setHosts(hosts);
    }
    
    CliUtils.replaceExistingOptions(options, newOptions);
    return ProcessResultType.SUCCEED;
  }

}

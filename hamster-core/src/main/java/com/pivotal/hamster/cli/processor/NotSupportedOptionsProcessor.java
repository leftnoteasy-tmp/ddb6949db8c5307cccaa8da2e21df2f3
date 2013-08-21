package com.pivotal.hamster.cli.processor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.hamster.cli.AppLaunchContext;
import com.pivotal.hamster.common.HamsterCliParseException;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.Option;

public class NotSupportedOptionsProcessor implements CliProcessor {
  private static final Log LOG = LogFactory.getLog(NotSupportedOptionsProcessor.class);

  final String[] NOT_SUPPPORTED_OPTIONS = new String[] {
      "am",
      "app",
      "bind-to",
      "bind-to-core",
      "bind-to-socket",
      "cf",
      "cpu-set",
      "disable-recovery",
      "do-not-resolve",
      "enable-recovery",
      "hetero-apps",
      "hetero-nodes",
      "launch-agent",
      "map-by",
      "max-restarts",
      "N",
      "no-local",
      "nooversubscribe",
      "oversubscribe",
      "noprefix",
      "npersocket",
      "ompi-server",       /* need verify */
      "output-filename",
      "output-proctable",
      "ppr",
      "rank-by",
      "report-bindings",
      "report-child-jobs-separatelly",
      "report-events",
      "rf",
      "server-wait-time", /* need verify */
      "stdin",
      "tag-output",
      "timestamp-output",
      "use-hwthread-cpus",
      "use-regexp",
      "wait-for-server",
      "wd",               /* need verify */
      "wdir",             /* need verify */
      "xml",
      "xml-file",
      "xterm",
      "maxproc",
      "max-node",
      "min-proc",
      "min-ppn"
  };
  
  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    Set<String> inOptionsSet = new HashSet<String>();
    for (Option op : options) {
      inOptionsSet.add(op.getOpt());
    }
    
    Set<String> unSupportedOptionsSet = new HashSet<String>();
    for (String op : NOT_SUPPPORTED_OPTIONS) {
      unSupportedOptionsSet.add(op);
    }
    
    boolean anythingUnsupported = false;
    
    // check if any option is not supported
    for (String inOp : inOptionsSet) {
      if (unSupportedOptionsSet.contains(inOp)) {
        anythingUnsupported = true;
        LOG.error("option:[" + inOp + "] is unsupported");
      }
    }
    
    if (anythingUnsupported) {
      LOG.error("one or more option is unsupported by hamster");
      throw new HamsterCliParseException("one or more option is unsupported by hamster");
    }
    
    return ProcessResultType.SUCCEED; 
  }

}

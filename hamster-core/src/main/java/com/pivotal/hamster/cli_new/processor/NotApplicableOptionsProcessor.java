package com.pivotal.hamster.cli_new.processor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.common.HamsterException;

/**
 * Handle options not-applicable, we will give a warn message to user and ignore such options
 */
public class NotApplicableOptionsProcessor implements CliProcessor {
  private static final Log LOG = LogFactory.getLog(NotApplicableOptionsProcessor.class);

  final String[] NOT_APPLICABLE_OPTIONS = new String[] {
      "do-not-launch",
      "leave-session-attached",
      "preload-files-dest-dir",
  };

  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    Set<String> inOptionsSet = new HashSet<String>();
    for (Option op : options) {
      inOptionsSet.add(op.getOpt());
    }
    
    Set<String> unApplicableOptionsSet = new HashSet<String>();
    for (String op : NOT_APPLICABLE_OPTIONS) {
      unApplicableOptionsSet.add(op);
    }
    
    // check if any option is not supported
    for (String inOp : inOptionsSet) {
      if (unApplicableOptionsSet.contains(inOp)) {
        LOG.warn("option:[" + inOp + "] is not applicable, we will ignore this.");
      }
    }
    
    return ProcessResultType.SUCCEED;
  }

}

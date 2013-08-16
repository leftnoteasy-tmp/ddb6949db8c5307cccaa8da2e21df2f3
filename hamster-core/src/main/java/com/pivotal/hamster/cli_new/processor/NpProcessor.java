package com.pivotal.hamster.cli_new.processor;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.cli_new.utils.CliUtils;
import com.pivotal.hamster.common.HamsterCliParseException;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.Option;

/**
 * Check if -np specified
 */
public class NpProcessor implements CliProcessor {
  private static final Log LOG = LogFactory.getLog(NpProcessor.class);

  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    int np = Integer.MIN_VALUE;
    
    // check -c --c
    Option op = CliUtils.getOption("c", options);
    if (null != op) {
      np = Integer.parseInt(op.getValue());
    }
    
    // check -n, --n
    op = CliUtils.getOption("n", options);
    if (null != op) {
      int value = Integer.parseInt(op.getValue());
      if (np != Integer.MIN_VALUE && np != value) {
        LOG.error("two options specified number of processors to run, but with different value");
        throw new HamsterCliParseException("two options specified number of processors to run, but with different value");
      }
    }
    
    // check -np, --np
    op = CliUtils.getOption("np", options);
    if (null != op) {
      int value = Integer.parseInt(op.getValue());
      if (np != Integer.MIN_VALUE && np != value) {
        LOG.error("two options specified number of processors to run, but with different value");
        throw new HamsterCliParseException("two options specified number of processors to run, but with different value");
      }
    }
    
    // check -max-vm-size, --max-vm-size
    op = CliUtils.getOption("max-vm-size", options);
    if (null != op) {
      int value = Integer.parseInt(op.getValue());
      if (np != Integer.MIN_VALUE && np != value) {
        LOG.error("two options specified number of processors to run, but with different value");
        throw new HamsterCliParseException("two options specified number of processors to run, but with different value");
      }
    }
    
    if (np <= 0) {
      if (np == Integer.MIN_VALUE) {
        LOG.error("np not specified");
        throw new HamsterCliParseException("np not specified");
      }
      LOG.error(String.format("specified np=%d <= 0,", np));
      throw new HamsterCliParseException(String.format("specified np=%d <= 0,", np));
    }
    
    return ProcessResultType.SUCCEED;
  }

}

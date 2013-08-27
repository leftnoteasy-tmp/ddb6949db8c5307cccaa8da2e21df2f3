package com.pivotal.hamster.cli.processor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.hamster.cli.AppLaunchContext;
import com.pivotal.hamster.cli.utils.CliUtils;
import com.pivotal.hamster.common.HamsterCliParseException;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.Option;

/**
 * process options unique in hamster (not open-mpi)
 */
public class HamsterCliProcessor implements CliProcessor {
  private static final Log LOG = LogFactory.getLog(HamsterCliProcessor.class);

  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    List<Option> newOptions = new ArrayList<Option>();
    for (Option op : options) {
      if (StringUtils.equals("cpu", op.getOpt())) {
        context.setCpu(Integer.parseInt(op.getValue()));
      } else if (StringUtils.equals("mem", op.getOpt())) {
        context.setMem(Integer.parseInt(op.getValue()));
      } else if (StringUtils.equals("hvalgrind", op.getOpt())) {
        context.setValgrind(true);
      } else if (StringUtils.equals("max-proc", op.getOpt())) {
        // not supported yet
      } else if (StringUtils.equals("max-node", op.getOpt())) {
        // not supported yet
      } else if (StringUtils.equals("min-proc", op.getOpt())) {
        // not supported yet
      } else if (StringUtils.equals("max-ppn", op.getOpt())) {
        context.setMaxPpn(Integer.parseInt(op.getValue()));
      } else if (StringUtils.equals("min-ppn", op.getOpt())) {
        // not supported yet
      } else if (StringUtils.equals("max-at", op.getOpt())) {
        int maxAt = Integer.parseInt(op.getValue());
        if (maxAt <= 0) {
          LOG.error(String.format("-max-at must > 0, now=%d", maxAt));
          throw new HamsterCliParseException(String.format("-max-at must > 0, now=%d", maxAt));
        }
        context.setMaxAt(maxAt);
      } else if (StringUtils.equals("p", op.getOpt())) {
        String policy = op.getValue();
        boolean valid = false;
        if (StringUtils.equalsIgnoreCase(policy, "default") || StringUtils.equalsIgnoreCase(policy, "cl") || StringUtils.equalsIgnoreCase(policy, "compute-locality")) {
          valid = true;
        }
        if (!valid) {
          LOG.error("policy is not valid, please run hamster -h get more info");
          throw new HamsterCliParseException("policy is not valid, please run hamster -h get more info");
        }
        context.setPolicy(policy);
      } else {
        newOptions.add(op);
      }
    }
    
    CliUtils.replaceExistingOptions(options, newOptions);
    return ProcessResultType.SUCCEED;
  }
  
}

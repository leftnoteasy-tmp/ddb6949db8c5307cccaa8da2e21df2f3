package com.pivotal.hamster.cli_new.processor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.cli_new.utils.CliUtils;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.Option;

/**
 * process options unique in hamster (not open-mpi)
 */
public class HamsterCliProcessor implements CliProcessor {

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
        context.setMaxAt(Integer.parseInt(op.getValue()));
      } else {
        newOptions.add(op);
      }
    }
    
    CliUtils.replaceExistingOptions(options, newOptions);
    return ProcessResultType.SUCCEED;
  }
  
}

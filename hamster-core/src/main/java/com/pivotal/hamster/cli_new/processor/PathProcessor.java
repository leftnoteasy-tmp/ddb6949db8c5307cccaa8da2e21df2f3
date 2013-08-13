package com.pivotal.hamster.cli_new.processor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.cli_new.utils.CliUtils;
import com.pivotal.hamster.common.HamsterException;

public class PathProcessor implements CliProcessor {

  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    List<Option> newOptions = new ArrayList<Option>();
    
    for (Option op : options) {
      if (StringUtils.equals("path", op.getOpt())) {
        context.appendEnv("PATH", op.getValue());
      } else {
        newOptions.add(op);
      }
    }
    
    CliUtils.replaceExistingOptions(options, newOptions);
    return ProcessResultType.SUCCEED;
  }

}

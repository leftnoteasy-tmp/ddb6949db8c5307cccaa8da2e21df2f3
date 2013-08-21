package com.pivotal.hamster.cli.processor;

import java.util.List;

import com.pivotal.hamster.cli.AppLaunchContext;
import com.pivotal.hamster.cli.utils.CliUtils;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.Option;

public class HelpProcessor implements CliProcessor {

  @Override
  public ProcessResultType process(List<Option> options, AppLaunchContext context)
      throws HamsterException {
    if (CliUtils.containsOption("h", options)) {
      return ProcessResultType.HELP_TERMINATED;
    }
    return ProcessResultType.SUCCEED;
  }
}

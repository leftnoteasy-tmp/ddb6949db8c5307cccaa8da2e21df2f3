package com.pivotal.hamster.cli_new.processor;

import java.util.List;

import org.apache.commons.cli.Option;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.cli_new.utils.CliUtils;
import com.pivotal.hamster.common.HamsterException;

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

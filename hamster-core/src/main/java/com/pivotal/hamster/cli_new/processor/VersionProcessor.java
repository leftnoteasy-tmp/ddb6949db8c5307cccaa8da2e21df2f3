package com.pivotal.hamster.cli_new.processor;

import java.util.List;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.cli_new.utils.CliUtils;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.Option;

public class VersionProcessor implements CliProcessor {

  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    if (CliUtils.containsOption("V", options)) {
      return ProcessResultType.VERSION_TERMINATED;
    }
    return ProcessResultType.SUCCEED;
  }

}

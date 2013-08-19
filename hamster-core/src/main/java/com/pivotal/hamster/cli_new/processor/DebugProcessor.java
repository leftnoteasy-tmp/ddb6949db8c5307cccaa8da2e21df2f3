package com.pivotal.hamster.cli_new.processor;

import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.Option;

public class DebugProcessor implements CliProcessor {

  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    for (Option op : options) {
      if (StringUtils.equals("debug", op.getOpt()) || StringUtils.equals("tv", op.getOpt())) {
        context.setDebug(true);
      }
    }
    return ProcessResultType.SUCCEED;
  }

}

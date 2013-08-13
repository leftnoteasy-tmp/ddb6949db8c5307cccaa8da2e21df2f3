package com.pivotal.hamster.cli_new.processor;

import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.common.HamsterException;

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

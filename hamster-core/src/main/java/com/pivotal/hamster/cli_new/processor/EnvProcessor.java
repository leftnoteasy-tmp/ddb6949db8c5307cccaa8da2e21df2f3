package com.pivotal.hamster.cli_new.processor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.cli_new.utils.CliUtils;
import com.pivotal.hamster.common.HamsterCliParseException;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.Option;

public class EnvProcessor implements CliProcessor {
  private static final Log LOG = LogFactory.getLog(EnvProcessor.class);

  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    List<Option> newOptions = new ArrayList<Option>();
    
    for (Option op : options) {
      if (StringUtils.equals("x", op.getOpt())) {
        String env = op.getValue();
        if (env.indexOf('=') < 0) {
          LOG.error("env must in key=value format, " + env);
          throw new HamsterCliParseException("env must in key=value format, " + env);
        }
        String key = env.substring(0, env.indexOf('='));
        String value = env.substring(env.indexOf('=') + 1);
        context.appendEnv(key, value);
      } else {
        newOptions.add(op);
      }
    }
    
    CliUtils.replaceExistingOptions(options, newOptions);
    return ProcessResultType.SUCCEED;
  }

}

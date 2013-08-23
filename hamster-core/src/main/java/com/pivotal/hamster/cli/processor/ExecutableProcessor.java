package com.pivotal.hamster.cli.processor;

import java.io.File;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.hamster.cli.AppLaunchContext;
import com.pivotal.hamster.common.HamsterCliParseException;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.Option;

/**
 * check if user's application specified
 */
public class ExecutableProcessor implements CliProcessor {
  private static final Log LOG = LogFactory.getLog(ExecutableProcessor.class);

  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    if (context.getArgs() == null || context.getArgs().length == 0) {
      LOG.error("No executable was specified on the hamster command line.");
      throw new HamsterCliParseException(
          "No executable was specified on the hamster command line.");
    }
    
    String appName = context.getArgs()[0];
    File file = new File(appName);
    if (file.isAbsolute()) {
      if (!file.exists()) {
        LOG.warn(String.format(
            "you specified %s as your app, but this file not"
                + " exist in current node, all executable must exists in all"
                + " NodeManager in your cluster with same path", appName));
      }
    } else {
      LOG.warn("we note that you used relative path for you executable, "
          + " it's better to use absolute path instead");
    }
    
    return ProcessResultType.SUCCEED;
  }

}

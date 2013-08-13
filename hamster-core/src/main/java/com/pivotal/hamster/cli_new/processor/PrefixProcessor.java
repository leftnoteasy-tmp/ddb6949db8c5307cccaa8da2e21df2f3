package com.pivotal.hamster.cli_new.processor;

import java.io.File;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.cli_new.utils.CliUtils;
import com.pivotal.hamster.common.HamsterCliParseException;
import com.pivotal.hamster.common.HamsterConfig;
import com.pivotal.hamster.common.HamsterException;

public class PrefixProcessor implements CliProcessor {
  private static final Log LOG = LogFactory.getLog(PrefixProcessor.class);

  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    Option prefixOption = CliUtils.getOption("prefix", options);
    
    // just return if no this option specified
    if (prefixOption == null) {
      if (context.getPrefix() == null) {
        LOG.error("prefix not specified, you can specify it in either " + HamsterConfig.OMPI_HOME_PROPERTY_KEY + ", or --prefix");
        throw new HamsterCliParseException("prefix not specified, you can specify it in either " + HamsterConfig.OMPI_HOME_PROPERTY_KEY + ", or --prefix");
      }
    }
    
    String prefix = prefixOption == null ? context.getPrefix() : prefixOption.getValue();
    if (!prefix.startsWith("/")) {
      // transfer prefix to abs path
      File file = new File(prefix);
      prefix = file.getAbsolutePath();
    }
    
    // we will check if "mpirun" under $prefix/bin
    File mpirunFile = new File(new File(prefix, "bin"), "mpirun");
    if (!mpirunFile.exists()) {
      // just give a warn to user because current node MAY not in yarn NM set
      LOG.warn("cannot find mpirun in location:" + mpirunFile.getAbsolutePath()
          + ", but make sure open-mpi installed in all NMs in your cluster");
    }
    
    // add to $PATH, $LIBRARY_PATH, $DYLD_LIBRARY_PATH
    context.appendEnv("PATH", new File(prefix, "bin").getAbsolutePath());
    context.appendEnv("LIBRARY_PATH", new File(prefix, "lib").getAbsolutePath());
    context.appendEnv("DYLD_LIBRARY_PATH", new File(prefix, "lib").getAbsolutePath());
    
    // remove --prefix option, because we will manage this ourselves
    List<Option> newOptions = CliUtils.removeOptions(new String[] { "prefix" } , options);
    CliUtils.replaceExistingOptions(options, newOptions);
    
    return ProcessResultType.SUCCEED;
  }

}

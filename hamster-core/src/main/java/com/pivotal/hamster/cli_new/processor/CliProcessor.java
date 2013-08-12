package com.pivotal.hamster.cli_new.processor;

import org.apache.commons.cli.CommandLine;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.common.HamsterException;

public interface CliProcessor {
  public void process(CommandLine in,
      AppLaunchContext context) throws HamsterException;
}

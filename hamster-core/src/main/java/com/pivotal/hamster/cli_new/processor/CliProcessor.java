package com.pivotal.hamster.cli_new.processor;

import java.util.List;

import org.apache.commons.cli.Option;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.common.HamsterException;

public interface CliProcessor {
  /**
   * Type of the process result, 
   * Succeed : not any error occurred in this processor, and program will continue
   * Help_Terminated : not any error, but program will not continue, it will show help to user
   * Failed : processor will throw exception 
   */
  public static enum ProcessResultType {
    SUCCEED,
    HELP_TERMINATED
  }
  
  /**
   * Process option and add values to AppLaunchContext if needed
   * @param options, options to check and change
   * @param context, context for launch AM
   * @return true if process ended
   * @throws HamsterException
   */
  
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException;
}

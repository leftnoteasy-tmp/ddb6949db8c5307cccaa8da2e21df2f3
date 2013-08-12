package com.pivotal.hamster.cli_new.processor;

import org.apache.commons.cli.CommandLine;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.common.HamsterException;

/**
 * we need merge some same function options with different name, like -n, -np
 */
public class MergeOptionsProcessor implements CliProcessor {
  
  /**
   * how do we do such merge, we need
   * 1) specify an array of short options
   * 2) specify an array of long options (null if no long options)
   * 3) specify an output of short option
   * 4) specify an output of long option (null if no output long option)
   */
  static class MergeSpec {
    String[] ops;
    String outShortOption;
    String outLongOption;
    boolean hasArg;
    
    MergeSpec(String[] shortOptions,
        String outShortOption, String outLongOption, boolean hasArg) {
      this.ops = shortOptions;
      this.outShortOption = outShortOption;
      this.outLongOption = outLongOption;
      this.hasArg = hasArg;
    }
  }
  
  final static MergeSpec[] PREDEFINED_MERGE_SPECS = new MergeSpec[] {
    // insert for -np series 
    new MergeSpec(new String[] { "c", "np", "max-vm-size", "n" }, 
                  "np", "np", true),
    
    // insert for -d, -debug-devel
    new MergeSpec(new String[] { "d", "debug-devel" },
                  "debug-devel",
                  "debug-devel", false),
    
    // insert for -H, -host
    new MergeSpec(new String[] { "H", "host" },
                  "host", "host", true),
    
    // insert for -hostfile, -machinefile
    new MergeSpec(new String[] { "hostfile", "machinefile" },
                  "hostfile", "hostfile", true),
    
    // insert for -debug and -tv
    new MergeSpec(new String[] { "debug", "tv" },
                  "debug", "debug", false)
  };

  @Override
  public void process(CommandLine in, AppLaunchContext context)
      throws HamsterException {
    // just a place holder, will not implement this!!
  }
}

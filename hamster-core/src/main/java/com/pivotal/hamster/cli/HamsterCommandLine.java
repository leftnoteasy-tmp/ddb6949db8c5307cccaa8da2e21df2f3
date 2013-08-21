package com.pivotal.hamster.cli;

import java.util.ArrayList;
import java.util.List;

import com.pivotal.hamster.commons.cli.CommandLine;
import com.pivotal.hamster.commons.cli.Option;

/**
 * CommandLine object for processing, I wrote this because the CommandLine in
 * commons-cli is a read-only object
 */
public class HamsterCommandLine {
  List<Option> options;
  List<String> args;
  
  public HamsterCommandLine(CommandLine cli) {
    // add options
    options = new ArrayList<Option>(); 
    for (Option op : cli.getOptions()) {
      options.add(op);
    }
    
    // add args
    args = new ArrayList<String>();
    for (String arg : cli.getArgs()) {
      args.add(arg);
    }
  }
  
  public HamsterCommandLine() {
    options = new ArrayList<Option>();
    args = new ArrayList<String>();
  }
  
  public List<Option> getOptions() {
    return options;
  }
  
  public List<String> getArgs() {
    return args;
  }
  
  public void setOptions(List<Option> options) {
    this.options = options;
  }
  
  public void setArgs(List<String> args) {
    this.args = args;
  }
}

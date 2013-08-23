package com.pivotal.hamster.cli.parser;

import java.util.ArrayList;
import java.util.List;

import com.pivotal.hamster.cli.AppLaunchContext;
import com.pivotal.hamster.commons.cli.CommandLine;
import com.pivotal.hamster.commons.cli.Option;
import com.pivotal.hamster.commons.cli.ParseException;

public class CliParserUTUtils {
  static CliParser parser;
  static List<Option> options = null;
  static AppLaunchContext context = null;
  
  public static void parseCli(String[] args) throws ParseException {
    parser = new CliParser("1.7");
    CommandLine cli = parser.parse(args);
    options = new ArrayList<Option>();
    for (Option op : cli.getOptions()) {
      options.add(op);
    }
    context = new AppLaunchContext();
    context.setArgs(cli.getArgs());
  }
  
  public static List<Option> getOptions() {
    return options;
  }
  
  public static AppLaunchContext getContext() {
    return context;
  }
}

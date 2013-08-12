package com.pivotal.hamster.cli_new;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import com.pivotal.hamster.cli_new.parser.CliParser;
import com.pivotal.hamster.cli_new.processor.CliProcessor;

public class HamsterCli {
  List<CliProcessor> cliProcessors;
  AppLaunchContext context;
  
  public HamsterCli() {
    initialize();
  }
  
  void initialize() {
    cliProcessors = new ArrayList<CliProcessor>();
    // cliProcessors.add(...);
  }
  
  public void processCli(String[] args) throws ParseException {
    // init CliParser do the first parsing
    CliParser parser = new CliParser("1.7");
    CommandLine cli = parser.parse(args);
    
    for (CliProcessor processor : cliProcessors) {
      processor.process(cli, context);
    }
  }
}

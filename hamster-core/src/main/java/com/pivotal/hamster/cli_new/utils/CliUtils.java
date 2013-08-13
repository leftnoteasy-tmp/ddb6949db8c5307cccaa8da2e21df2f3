package com.pivotal.hamster.cli_new.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.lang.StringUtils;

public class CliUtils {
  public static boolean containsOption(String opName, List<Option> options) {
    for (Option op : options) {
      if (StringUtils.equals(op.getOpt(), opName)) {
        return true;
      }
    }
    return false;
  }
  
  public static Option getOption(String opName, List<Option> options) {
    for (Option op : options) {
      if (StringUtils.equals(op.getOpt(), opName)) {
        return op;
      }
    }
    return null;
  }
  
  public static List<Option> removeOptions(String[] optionsToRemove, List<Option> options) {
    Set<String> toRemoveSet = new HashSet<String>();
    for (String op : optionsToRemove) {
      toRemoveSet.add(op);
    }
    
    List<Option> newOptions = new ArrayList<Option>();
    for (Option op : options) {
      if (!toRemoveSet.contains(op.getOpt())) {
        newOptions.add(op);
      }
    }
    
    return newOptions;
  }
  
  public static void replaceExistingOptions(List<Option> originOptions, List<Option> newOptions) {
    originOptions.clear();
    originOptions.addAll(newOptions);
  }
}

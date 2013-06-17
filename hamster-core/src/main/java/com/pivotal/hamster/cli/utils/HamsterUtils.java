package com.pivotal.hamster.cli.utils;

public class HamsterUtils {
  public static String appendEnv(String env, String append) {
    if (env == null || env.isEmpty()) {
      return append;
    }
    
    if (append == null || append.isEmpty()) {
      return env;
    }
    
    if (env.endsWith(":")) {
      return env + append;
    }
    
    return env + ":" + append;
  }
}

package com.pivotal.hamster.cli.parser;

public class ParserUtils {
  /**
   * return is a valid option
   */
  public static boolean isOption(String key) {
    if (key.startsWith("--") && (key.length() > 2) && (key.charAt(2) != '-')) {
      return true;
    }
    
    if (key.startsWith("-") && (key.length() > 1) && (key.charAt(1) != '-')) {
      return true;
    }
    
    return false;
  }
  
  /**
   * return key of a option,
   * return null if it's not a valid option
   */
  public static String getOptionKey(String key) {
    if (key.startsWith("--") && (key.length() > 2) && (key.charAt(2) != '-')) {
      return key.substring(2);
    }
    
    if (key.startsWith("-") && (key.length() > 1) && (key.charAt(1) != '-')) {
      return key.substring(1);
    } 
    
    return null;
  }
}

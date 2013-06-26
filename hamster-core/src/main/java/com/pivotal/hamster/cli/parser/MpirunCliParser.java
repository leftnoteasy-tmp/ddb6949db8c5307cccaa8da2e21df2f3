package com.pivotal.hamster.cli.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class MpirunCliParser implements CliParser {

  public String[] parse(String[] args, HamsterParamBuilder builder)
      throws IOException {
    // we will ignore the first one, because it should be "mpirun"
    List<String> output = new ArrayList<String>();
    output.add(args[0]);
    int offset = 1;
    boolean npFound = false;
    
    while (offset < args.length) {
      if (StringUtils.equals(args[offset], "-np") || 
          StringUtils.equals(args[offset], "-c") ||
          StringUtils.equals(args[offset], "--n") ||
          StringUtils.equals(args[offset], "-n")) {
        output.add(args[offset]);
        
        // if we already found a -np #, we will skip followed params
        if (npFound) {
          offset++;
          continue;
        }
        
        // get value
        offset++;
        if (offset >= args.length) {
          throw new IOException("invalid -np# param specified");
        }
        String value = args[offset];
        output.add(args[offset]);
       
        // add -np to builder
        builder.np = Integer.parseInt(value);
        
        npFound = true;
      } else {
        // add it to output param
        output.add(args[offset]);
      }
      
      offset++;
    }
    
    return output.toArray(new String[0]);
  }

}

package com.pivotal.hamster.cli.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class McaCliParser implements CliParser {

  public String[] parse(String[] args, HamsterParamBuilder builder)
      throws IOException {
    // we will ignore the first one, because it should be "mpirun"
    List<String> output = new ArrayList<String>();
    output.add(args[0]);
    int offset = 1;
    
    while (offset < args.length) {
      if (StringUtils.equals(args[offset], "-mca")) {
        offset++;
        if (offset >= args.length) {
          throw new IOException("invalid mca param specify");
        }
        String key = args[offset];
        
        offset++;
        if (offset >= args.length) {
          throw new IOException("invalid mca param specify");
        }
        String value = args[offset];
        
        // add k,v to builder
        builder.mcaParams.put(key, value);
      } else {
        // add it to output param
        output.add(args[offset]);
      }
      
      offset++;
    }
    
    return output.toArray(new String[0]);
  }

}

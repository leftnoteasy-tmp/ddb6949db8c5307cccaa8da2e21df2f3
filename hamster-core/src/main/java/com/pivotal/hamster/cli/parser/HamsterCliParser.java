package com.pivotal.hamster.cli.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.pivotal.hamster.cli.utils.HamsterUtils;

public class HamsterCliParser implements CliParser {

  public String[] parse(String[] args, HamsterParamBuilder builder)
      throws IOException {
    // we will ignore the first one, because it should be "mpirun"
    List<String> output = new ArrayList<String>();
    output.add(args[0]);
    int offset = 1;
    
    while (offset < args.length) {
      if (StringUtils.equals(args[offset], "--hamster-verbose")) {
        /*
         * process --hamster-verbose
         */
        builder.verbose = true;
      } else if (StringUtils.equals(args[offset], "--hamster-valgrind")) {
        builder.valgrind = true;
      } else if (StringUtils.equals(args[offset], "--add-file") || StringUtils.equals(args[offset], "--add-archive")) {
        /*
         * process --add-file, --add-archive
         */
        boolean isFile = StringUtils.equals(args[offset], "--add-file");
        
        offset++;
        if (offset >= args.length) {
          throw new IOException("failed to parse --add-file or --add-archieve");
        }
        // check if the file exists
        String filename = args[offset];
        
        // add it to file/archieve
        if (isFile) {
          builder.addFiles.add(filename);
        } else {
          builder.addArchives.add(filename);
        }
      } else if (StringUtils.equals(args[offset], "--add-env")) {
        /*
         * process --add-env
         */
        offset++;
        if (offset >= args.length) {
          throw new IOException("failed to parse --add-env");
        }
        String env = args[offset];
        if (!env.contains("=")) {
          throw new IOException("env to be added should in format KEY=VALUE, but now is," + env);
        }
        String key = env.substring(0, env.indexOf('='));
        String value = env.substring(env.indexOf('=') + 1);
        if (!builder.userEnvs.containsKey(key)) {
          // just insert it
          builder.userEnvs.put(key, value);
        } else {
          // append the new one and insert it
          value = HamsterUtils.appendEnv(builder.userEnvs.get(key), value);
          builder.userEnvs.put(key, value);
        }
			} else if (StringUtils.equals(args[offset], "--hamster-mem")) {
				offset++;
				if (offset >= args.length) {
					throw new IOException("failed to parse --hamster-mem");
				}

				if (args[offset] != null && !args[offset].isEmpty()) {
					builder.setHamsterMemory(args[offset]);
				}
			} else if (StringUtils.equals(args[offset], "--hamster-cpu")) {
				offset++;
				if (offset >= args.length) {
					throw new IOException("failed to parse --hamster-cpu");
				}

				if (args[offset] != null && !args[offset].isEmpty()) {
					builder.setHamsterCPU(args[offset]);
				}
      } else {
        output.add(args[offset]);
      }
      offset++;
    }
    
    return output.toArray(new String[0]);
  }

}

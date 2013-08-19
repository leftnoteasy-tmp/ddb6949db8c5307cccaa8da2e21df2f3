package com.pivotal.hamster.cli_new.processor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.pivotal.hamster.cli_new.AppLaunchContext;
import com.pivotal.hamster.cli_new.utils.CliUtils;
import com.pivotal.hamster.common.HamsterException;
import com.pivotal.hamster.commons.cli.Option;

public class PreloadFilesProcessor implements CliProcessor {

  @Override
  public ProcessResultType process(List<Option> options,
      AppLaunchContext context) throws HamsterException {
    List<Option> newOptions = new ArrayList<Option>();
    for (Option op : options) {
      if (StringUtils.equals("preload-files", op.getOpt()) || StringUtils.equals("s", op.getOpt())) {
        context.addCommaSplitFiles(op.getValue());
        continue;
      }
      
      if (StringUtils.equals("preload-archives", op.getOpt())) {
        context.addCommaSplitArchives(op.getValue());
        continue;
      }
      
      newOptions.add(op);
    }
    
    CliUtils.replaceExistingOptions(options, newOptions);
    return ProcessResultType.SUCCEED;
  }

}

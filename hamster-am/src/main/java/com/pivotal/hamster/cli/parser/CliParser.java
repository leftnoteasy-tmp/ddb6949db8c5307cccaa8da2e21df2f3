package com.pivotal.hamster.cli.parser;

import java.io.IOException;

public interface CliParser {
  /**
   * parse params, save result to builder, and return param after parse
   * @param args, input
   * @return args left after parse
   * @throws IOException
   */
  String[] parse(String[] args, HamsterParamBuilder builder) throws IOException;
}

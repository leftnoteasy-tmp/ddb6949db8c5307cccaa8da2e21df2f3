package com.pivotal.hamster.appmaster.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.ipc.YarnRPC;

public class HadoopRpcUtils {
  private static YarnRPC rpc = null;
  
  public static synchronized YarnRPC getYarnRPC(Configuration conf) {
    if (rpc == null) {
      rpc = YarnRPC.create(conf);
    }
    return rpc;
  }
}

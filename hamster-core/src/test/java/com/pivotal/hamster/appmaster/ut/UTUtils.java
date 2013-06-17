package com.pivotal.hamster.appmaster.ut;

import org.apache.hadoop.yarn.api.records.Resource;

public class UTUtils {
  public static Resource GetDefaultResource() {
    return new Resource() {
      @Override
      public int compareTo(Resource res) {
        return 0;
      }

      @Override
      public int getMemory() {
        return 1024;
      }

      @Override
      public int getVirtualCores() {
        return 1;
      }

      @Override
      public void setMemory(int arg0) {
      }

      @Override
      public void setVirtualCores(int arg0) {
      }      
    };
  }
}

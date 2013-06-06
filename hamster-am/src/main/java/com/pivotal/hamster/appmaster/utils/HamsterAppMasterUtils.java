package com.pivotal.hamster.appmaster.utils;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class HamsterAppMasterUtils {
  public static ApplicationAttemptId getAppAttemptIdFromEnv() {
    return getContainerIdFromEnv().getApplicationAttemptId();
  }
  
  public static ContainerId getContainerIdFromEnv() {
    String containerIdStr =
        System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV);
    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    return containerId;
  }
}

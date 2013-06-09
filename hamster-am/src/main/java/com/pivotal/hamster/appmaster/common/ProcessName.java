package com.pivotal.hamster.appmaster.common;

import com.pivotal.hamster.proto.HamsterProtos.ProcessNameProto;

public class ProcessName {
  int jobId;
  int vpId;
  static final int BIG_PRIME = 502357;
  
  public ProcessName(ProcessNameProto proto) {
    this.jobId = proto.getJobid();
    this.vpId = proto.getVpid();
  }
  
  public int getJobId() {
    return jobId;
  }
  
  public int getVpId() {
    return vpId;
  }
  
  public ProcessNameProto getProcessNameProto() {
    return ProcessNameProto.newBuilder().setJobid(jobId).setVpid(vpId).build();
  }
  
  @Override
  public int hashCode() {
    return jobId * BIG_PRIME + vpId;
  }
}

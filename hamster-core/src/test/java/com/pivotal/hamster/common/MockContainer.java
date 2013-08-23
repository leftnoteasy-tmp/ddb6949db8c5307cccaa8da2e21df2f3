package com.pivotal.hamster.common;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

public class MockContainer implements Container {
  int id;
  String host = null;
  
  static class MockContainerId extends ContainerId {
    int cid;
    
    public MockContainerId(int cid) {
      this.cid = cid;
    }
    
    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return null;
    }

    @Override
    public int getId() {
      return cid;
    }

    @Override
    public void setApplicationAttemptId(ApplicationAttemptId arg0) {
    }

    @Override
    public void setId(int id) {
      cid = id;
    }
    
  }
  
  public MockContainer(int id) {
    this.id = id;
  }
  
  public MockContainer(int id, String host) {
    this.id = id;
    this.host = host;
  }

  @Override
  public int compareTo(Container o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ContainerStatus getContainerStatus() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ContainerToken getContainerToken() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ContainerId getId() {
    return new MockContainerId(id);
  }

  @Override
  public String getNodeHttpAddress() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NodeId getNodeId() {
    return new NodeId() {

      @Override
      public String getHost() {
        return host == null ? "localhost" : host;
      }

      @Override
      public int getPort() {
        return 0;
      }

      @Override
      public void setHost(String arg0) {        
      }

      @Override
      public void setPort(int arg0) {
      }
    };
  }

  @Override
  public Priority getPriority() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Resource getResource() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ContainerState getState() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setContainerStatus(ContainerStatus arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setContainerToken(ContainerToken arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setId(ContainerId arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setNodeHttpAddress(String arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setNodeId(NodeId arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setPriority(Priority arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setResource(Resource arg0) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setState(ContainerState arg0) {
    // TODO Auto-generated method stub
    
  }

}

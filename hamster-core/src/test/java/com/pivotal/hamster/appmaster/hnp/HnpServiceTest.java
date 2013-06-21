package com.pivotal.hamster.appmaster.hnp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.junit.Test;

import com.google.protobuf.Message;
import com.pivotal.hamster.appmaster.allocator.ContainerAllocator;
import com.pivotal.hamster.appmaster.allocator.MockContainerAllocator;
import com.pivotal.hamster.appmaster.common.MockContainer;
import com.pivotal.hamster.appmaster.event.HamsterEventType;
import com.pivotal.hamster.appmaster.hnp.HnpService.HnpState;
import com.pivotal.hamster.appmaster.launcher.ContainerLauncher;
import com.pivotal.hamster.appmaster.launcher.MockContainerLauncher;
import com.pivotal.hamster.appmaster.ut.MockDispatcher;
import com.pivotal.hamster.appmaster.ut.UTUtils;
import com.pivotal.hamster.common.HamsterContainer;
import com.pivotal.hamster.common.LaunchContext;
import com.pivotal.hamster.proto.HamsterProtos.AllocateRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.AllocateResponseProto;
import com.pivotal.hamster.proto.HamsterProtos.FinishRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.FinishResponseProto;
import com.pivotal.hamster.proto.HamsterProtos.HamsterHnpRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.LaunchContextProto;
import com.pivotal.hamster.proto.HamsterProtos.LaunchRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.LaunchResponseProto;
import com.pivotal.hamster.proto.HamsterProtos.MsgType;
import com.pivotal.hamster.proto.HamsterProtos.ProcessNameProto;
import com.pivotal.hamster.proto.HamsterProtos.RegisterRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.RegisterResponseProto;

public class HnpServiceTest {
  static class HnpServiceUT extends DefaultHnpService {
    boolean completed = false;
    boolean failed = false;

    public HnpServiceUT(Dispatcher dispatcher, ContainerAllocator containerAllocator,
        ContainerLauncher containerLauncher, HnpLivenessMonitor mon) {
      super(dispatcher, containerAllocator, containerLauncher, mon);
    }
    
    @Override
    String getNormalizedHost(String host) throws UnknownHostException {
      return host;
    }
  }
  
  static class MockClient {
    Socket client;
    DataInputStream is;
    DataOutputStream os;
    
    public MockClient(int port) throws UnknownHostException, IOException {
      client = new Socket("localhost", port);
      is = new DataInputStream(client.getInputStream());
      os = new DataOutputStream(client.getOutputStream());
    }
    
    public void writeHandshake() throws IOException {
      os.write(HnpService.HANDSHAKE_MSG.getBytes());
    }
    
    public void recvAndValidHandshake() throws IOException {
      for (int i = 0; i < HnpService.HANDSHAKE_MSG.getBytes().length; i++) {
        byte b = is.readByte();
        Assert.assertEquals(b, HnpService.HANDSHAKE_MSG.getBytes()[i]);
      }
    }
    
    public void writeMsg(MsgType type, Message msg) throws IOException {
      HamsterHnpRequestProto hnpRequest = HamsterHnpRequestProto.newBuilder().setMsgType(type).setRequest(msg.toByteString()).build();
      os.writeInt(hnpRequest.getSerializedSize());
      hnpRequest.writeTo(os);
      os.flush();
    }
    
    public boolean isSucceedInResponse() throws IOException {
      byte b = is.readByte();
      return b == HnpService.SUCCEED;
    }
    
    public byte[] readMsg() throws IOException {
      int len = is.readInt();
      byte[] msg = new byte[len];
      
      int offset = 0;
      while (offset < len) {
        int rc = is.read(msg, offset, len - offset);
        Assert.assertTrue(rc >= 0);
        offset += rc;
      }
      
      return msg;
    }
  }
  
  @Test
  public void testCompletedHnpService() throws Exception {
    // create service
    MockContainerAllocator allocator = new MockContainerAllocator();
    MockContainerLauncher launcher = new MockContainerLauncher();
    MockHnpLivenessMonitor monitor = new MockHnpLivenessMonitor();
    MockDispatcher dispatcher = new MockDispatcher();
    HnpServiceUT service = new HnpServiceUT(dispatcher, allocator, launcher, monitor);
    Assert.assertEquals(HnpState.Init, service.state);
    
    // launch service
    Configuration conf = new Configuration();
    service.init(conf);
    service.start();
    
    monitor.init(conf);
    monitor.start();
    
    // create a client, and handshake
    MockClient client = new MockClient(service.getServerPort());
    client.writeHandshake();
    client.recvAndValidHandshake();
    
    // write a register msg
    client.writeMsg(MsgType.REGISTER, RegisterRequestProto.newBuilder().build());
    
    // check recv msg
    Assert.assertTrue(client.isSucceedInResponse());
    byte[] msg = client.readMsg();
    RegisterResponseProto.parseFrom(msg);
    
    Assert.assertEquals(HnpState.Registered, service.state);
    
    // allocate
    // 1 - mock and set allocate result
    Map<String, List<HamsterContainer>> allocateResult = new HashMap<String, List<HamsterContainer>>();
    List<HamsterContainer> host1Containers = new ArrayList<HamsterContainer>();
    host1Containers.add(new HamsterContainer(new MockContainer(0), UTUtils.GetDefaultResource()));
    host1Containers.add(new HamsterContainer(new MockContainer(1), UTUtils.GetDefaultResource()));
    host1Containers.add(new HamsterContainer(new MockContainer(2), UTUtils.GetDefaultResource()));
    allocateResult.put("host1", host1Containers);
    List<HamsterContainer> host2Containers = new ArrayList<HamsterContainer>();
    host2Containers.add(new HamsterContainer(new MockContainer(3), UTUtils.GetDefaultResource()));
    host2Containers.add(new HamsterContainer(new MockContainer(4), UTUtils.GetDefaultResource()));
    allocateResult.put("host2", host2Containers);
    allocator.setAllocateResult(allocateResult);
    
    // 2 - client check allocated result
    client.writeMsg(MsgType.ALLOCATE, AllocateRequestProto.newBuilder().setResourceCount(5).build());
    Assert.assertTrue(client.isSucceedInResponse());
    msg = client.readMsg();
    AllocateResponseProto allocateResponse = AllocateResponseProto.parseFrom(msg);
    Map<String, Integer> hostToSlot = new HashMap<String, Integer>();

    Assert.assertEquals(2, allocateResponse.getNodeResourcesCount());
    hostToSlot.put(allocateResponse.getNodeResources(0).getHostName(), allocateResponse.getNodeResources(0).getSlot());
    hostToSlot.put(allocateResponse.getNodeResources(1).getHostName(), allocateResponse.getNodeResources(1).getSlot());
    Assert.assertEquals(Integer.valueOf(2), hostToSlot.get("host1"));
    Assert.assertEquals(Integer.valueOf(1), hostToSlot.get("host2"));
    Assert.assertEquals(HnpState.Allocated, service.state);
    
    // launch
    // 1 - generate launch request
    LaunchRequestProto.Builder launchBuilder = LaunchRequestProto.newBuilder();
    launchBuilder.addLaunchContexts(LaunchContextProto.newBuilder().addEnvars("KEY1=VAL1").addEnvars("KEY2=VAL2").
        setArgs("hello param").
        setHostName("host1").
        setName(ProcessNameProto.newBuilder().setJobid(0).setVpid(0)));
    launchBuilder.addLaunchContexts(LaunchContextProto.newBuilder().addEnvars("KEY3=VAL3").addEnvars("KEY4=VAL4").
        setArgs("hello param").
        setHostName("host2").
        setName(ProcessNameProto.newBuilder().setJobid(0).setVpid(1)));
    launcher.setLaunchResult(new boolean[] {true, true});
    client.writeMsg(MsgType.LAUNCH, launchBuilder.build());
    Assert.assertTrue(client.isSucceedInResponse());
    
    // 2 - check launch context passed to launcher
    LaunchContext[] launchCtx = launcher.getLastRequest();
    Assert.assertEquals("host1", launchCtx[0].getHost());
    Assert.assertEquals("VAL1", launchCtx[0].getEnvars().get("KEY1"));
    Assert.assertEquals("hello param", launchCtx[0].getArgs());
    Assert.assertEquals(0, launchCtx[0].getContainer().getId().getId());
    Assert.assertEquals("host2", launchCtx[1].getHost());
    Assert.assertEquals("VAL3", launchCtx[1].getEnvars().get("KEY3"));
    Assert.assertEquals("hello param", launchCtx[1].getArgs());
    Assert.assertEquals(3, launchCtx[1].getContainer().getId().getId());
    
    // 3 - check client recv response
    msg = client.readMsg();
    LaunchResponseProto launchResponse = LaunchResponseProto.parseFrom(msg);
    Assert.assertEquals(2, launchResponse.getResultsCount());
    Assert.assertEquals(0, launchResponse.getResults(0).getName().getVpid());
    Assert.assertEquals(true, launchResponse.getResults(0).getSuccess());
    Assert.assertEquals(1, launchResponse.getResults(1).getName().getVpid());
    Assert.assertEquals(true, launchResponse.getResults(1).getSuccess());
    
    // finish hnp 
    client.writeMsg(MsgType.FINISH, FinishRequestProto.newBuilder().setSucceed(true).build());
    Assert.assertTrue(client.isSucceedInResponse());
    msg = client.readMsg();
    FinishResponseProto.parseFrom(msg);
    Thread.sleep(100); // make sure main thread mark finished
    Assert.assertNotNull(dispatcher.getRecvedEvent());
    Assert.assertEquals(HamsterEventType.SUCCEED, dispatcher.getRecvedEvent().getType());
  }
}

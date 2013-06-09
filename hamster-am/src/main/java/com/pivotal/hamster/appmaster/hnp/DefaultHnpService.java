package com.pivotal.hamster.appmaster.hnp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.google.protobuf.Message;
import com.pivotal.hamster.appmaster.allocator.ContainerAllocator;
import com.pivotal.hamster.appmaster.common.HamsterContainer;
import com.pivotal.hamster.appmaster.common.HamsterException;
import com.pivotal.hamster.appmaster.common.LaunchContext;
import com.pivotal.hamster.appmaster.common.ProcessName;
import com.pivotal.hamster.appmaster.common.CompletedContainer;
import com.pivotal.hamster.appmaster.launcher.ContainerLauncher;
import com.pivotal.hamster.appmaster.utils.HamsterAppMasterUtils;
import com.pivotal.hamster.proto.HamsterProtos.AllocateRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.AllocateResponseProto;
import com.pivotal.hamster.proto.HamsterProtos.FinishRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.FinishResponseProto;
import com.pivotal.hamster.proto.HamsterProtos.HamsterHnpRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.HeartbeatRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.HeartbeatResponseProto;
import com.pivotal.hamster.proto.HamsterProtos.LaunchContextProto;
import com.pivotal.hamster.proto.HamsterProtos.LaunchRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.LaunchResponseProto;
import com.pivotal.hamster.proto.HamsterProtos.LaunchResultProto;
import com.pivotal.hamster.proto.HamsterProtos.MsgType;
import com.pivotal.hamster.proto.HamsterProtos.NodeResourceProto;
import com.pivotal.hamster.proto.HamsterProtos.ProcessNameProto;
import com.pivotal.hamster.proto.HamsterProtos.ProcessStateProto;
import com.pivotal.hamster.proto.HamsterProtos.ProcessStatusProto;
import com.pivotal.hamster.proto.HamsterProtos.RegisterRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.RegisterResponseProto;

public class DefaultHnpService extends HnpService {
  private static final Log LOG = LogFactory.getLog(HnpService.class);
  private static ServerSocket server;
  Thread serviceThread;
  ContainerAllocator containerAllocator;
  ContainerLauncher containerLauncher;
  HnpLivenessMonitor mon;
  Map<String, List<HamsterContainer>> allocateResult;
  Map<Integer, ProcessName> containerIdToName;
  HnpState state = HnpState.Init;
  
  enum HnpState {
    Init,
    Registered,
    Allocated,
    Finished
  }
  
  public DefaultHnpService(ContainerAllocator containerAllocator, 
      ContainerLauncher containerLauncher, HnpLivenessMonitor mon) {
    super(DefaultHnpService.class.getName());
    this.containerAllocator = containerAllocator;
    this.containerLauncher = containerLauncher;
    this.mon = mon;
    
    allocateResult = null;
    containerIdToName = new HashMap<Integer, ProcessName>();
  }
  
  
  @Override
  public void init(Configuration conf) {
    try {
      server = new ServerSocket(0);
    } catch (Exception e) {
      LOG.error("error when init server socket", e);
      throw new YarnException(e);
    }
    LOG.info("init succeed");
  }
  
  @Override
  public void start() {
    serviceThread = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
          LOG.info("wait for HNP connect");
          Socket socket = server.accept();
          LOG.info("HNP connected");
          
          // first we will recv/send handshake message
          DataInputStream is = new DataInputStream(socket.getInputStream());
          DataOutputStream os = new DataOutputStream(socket.getOutputStream());
          recvHandshake(is);
          sendHandshake(os);
          LOG.info("read handshake from HNP completed");
          
          // we will continuously process msg
          while (true) {
            int serializedSize = is.readInt();
            byte[] msg = new byte[serializedSize];
            int offset = 0;
            while (offset < serializedSize) {
              int rc = is.read(msg, offset, serializedSize - offset);
              if (rc < 0) {
                LOG.error("failed to read next msg");
                throw new IOException("failed to read next msg");
              }
              offset += rc;
            }
            HamsterHnpRequestProto proto = HamsterHnpRequestProto.parseFrom(msg);
            if (!proto.hasMsgType() || (!proto.hasRequest())) {
              LOG.error("this proto doesn't contain msg type of request, what happened?");
              throw new IOException("this proto doesn't contain msg type of request, what happened?");
            }
                        
            MsgType type = proto.getMsgType();
            if (type == MsgType.ALLOCATE) {
              // process allocate
              checkStateMatch(HnpState.Registered);
              AllocateRequestProto request = AllocateRequestProto.parseFrom(proto.getRequest());
              int n = request.getResourceCount();
              doAllocate(os, n);
            } else if (type == MsgType.FINISH) {
              // process finish
              FinishRequestProto request = FinishRequestProto.parseFrom(proto.getRequest());
              boolean succeed = request.getSucceed();
              String diagnotiscMsg = request.getDiagnostics();
              doFinish(os, succeed, diagnotiscMsg);
            } else if (type == MsgType.LAUNCH) {
              // process launch
              checkStateMatch(HnpState.Allocated);
              LaunchRequestProto request = LaunchRequestProto.parseFrom(proto.getRequest());
              doLaunch(os, request);
            } else if (type == MsgType.REGISTER) {
              // process register
              checkStateMatch(HnpState.Init);
              RegisterRequestProto.parseFrom(proto.getRequest());
              doRegister(os);
            } else if (type == MsgType.HEARTBEAT) {
              // process heartbeat 
              mon.receivedPing(HnpLivenessMonitor.DEFAULT_EXPIRE);
              HeartbeatRequestProto.parseFrom(proto.getRequest());
              doHeartbeat(os);
            } else {
              doFailedResponse(os, "not a valid request type, type=" + type);
            }
          }
          
        } catch (Exception e) {
          LOG.fatal("exception in process socket between AM and HNP", e);
          handleFatal();
        }
      }
      
      private void recvHandshake(DataInputStream in) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < HANDSHAKE_MSG.length(); i++) {
          int rc = in.read();
          if (rc == -1) {
            throw new IOException("got EOF before finish read handshake msg");
          }
          sb.append((char)rc);
          if (rc != HANDSHAKE_MSG.charAt(i)) {
            throw new IOException("get wrong handshake msg:" + sb.toString());
          }
        }
      }
      
      private void sendHandshake(DataOutputStream out) throws Exception {
        out.write(HANDSHAKE_MSG.getBytes());
      }
      
    });
    serviceThread.start();
  }
  
  void handleFatal() {
    System.exit(1);
  }
  
  void handleSuccess() {
    LOG.info("HNP reported succeed, terminate job");
    System.exit(0);
  }
  
  void checkStateMatch(HnpState expected) {
    if (state != expected) {
      StringBuilder sb = new StringBuilder();
      sb.append("state of hnp not match, expected:[");
      sb.append(expected.name());
      sb.append("]");
      sb.append(" but actual is:[");
      sb.append(state.name());
      sb.append("]");
      String msg = sb.toString();
      LOG.error(msg);
      throw new HamsterException(msg);
    }
  }
  
  /**
   * write error to output pipe, consist of 
   * (1 byte) : 0, FAILED
   * (4 byte) : n : length of msg 
   * n bytes, msg content
   * @throws IOException 
   */
  void doFailedResponse(DataOutputStream os, String msg) throws IOException {
    os.writeByte(FAILED);
    os.writeInt(msg.length());
    os.write(msg.getBytes());
  }
  
  /**
   * write success response to output pipe, consist of
   * (1 bytes) : 1, SUCCEED
   * (4 bytes) : n, length of msg
   * n bytes, serialized msg
   * @throws IOException
   */
  void doSuccessResponse(DataOutputStream os, Message msg) throws IOException {
    os.writeByte(SUCCEED);
    os.writeInt(msg.toByteArray().length);
    msg.writeTo(os);
  }
  
  void doFinish(DataOutputStream os, boolean succeed, String diag) throws IOException {
    state = HnpState.Finished;
    if (diag == null) {
      diag = "";
    }
    FinishResponseProto response = FinishResponseProto.newBuilder().build();
    
    if (succeed) {
      LOG.info("HNP report succeed, diag=" + diag);
      doSuccessResponse(os, response);
      containerAllocator.completeHnp(FinalApplicationStatus.SUCCEEDED);
      handleSuccess();
    } else {
      doSuccessResponse(os, response);
      containerAllocator.completeHnp(FinalApplicationStatus.FAILED);
      throw new HamsterException("HNP report failed, diag=" + diag);
    }
  }
  
  void doRegister(DataOutputStream os) throws IOException {
    RegisterResponseProto response = RegisterResponseProto.newBuilder().build();
    doSuccessResponse(os, response);
    state = HnpState.Registered;
  }
  
  void doHeartbeat(DataOutputStream os) throws IOException {
    CompletedContainer[] completedProcess = containerAllocator.pullCompletedContainers();
    HeartbeatResponseProto.Builder builder = HeartbeatResponseProto.newBuilder();
    if (completedProcess == null || completedProcess.length == 0) {
      doSuccessResponse(os, builder.build());
    }
    for (CompletedContainer c : completedProcess) {
      ProcessName name = containerIdToName.get(c.getContainerId());
      if (name == null) {
        LOG.warn("get a completed container but not associate to a existing process, containerid=" + c.getContainerId());
        continue;
      }
      
      ProcessStatusProto proto = ProcessStatusProto.newBuilder().
          setName(name.getProcessNameProto()).
          setState(ProcessStateProto.COMPLETED).
          setExitValue(c.getExitValue()).build();
      builder.addCompletedProcesses(proto);
    }
    doSuccessResponse(os, builder.build());
    mon.receivedPing(HnpLivenessMonitor.MONITOR);
  }
  
  void doAllocate(DataOutputStream os, int n) throws IOException {
    // instanity check
    if (n <= 0) {
      String msg = "got a zero or negtive n when allocate, please check";
      doFailedResponse(os, msg);
      throw new HamsterException(msg);
    }
    
    // do allocate
    allocateResult = containerAllocator.allocate(n);
    if (allocateResult == null) {
      String msg = "got null allocateResult, please check";
      doFailedResponse(os, msg);
      throw new HamsterException(msg);
    }
    
    // set response
    AllocateResponseProto.Builder builder = AllocateResponseProto.newBuilder();
    for (Entry<String, List<HamsterContainer>> entry : allocateResult.entrySet()) {
      String host = entry.getKey();
      int nSlot = entry.getValue().size();
      if (!getIsLocalHost(host)) {
        nSlot--;
      }
      NodeResourceProto node = NodeResourceProto.newBuilder().setHostName(host).setSlot(nSlot).build();
      builder.addNodeResources(node);
    }
    AllocateResponseProto response = builder.build();
    
    doSuccessResponse(os, response);
    state = HnpState.Allocated;
    
    // start monitor liveness
    mon.registerExpire();
  }
  
  String getNormalizedHost(String host) throws UnknownHostException {
    return HamsterAppMasterUtils.normlizeHostName(host);
  }
  
  boolean getIsLocalHost(String host) {
    return HamsterAppMasterUtils.isLocalHost(host);
  }
  
  LaunchContext[] mapProcessToAllocatedContainers(List<LaunchContextProto> launchProtos) throws UnknownHostException {
    // make sure there's no duplicated launch request
    Set<ProcessName> processedNames = new HashSet<ProcessName>();
    List<LaunchContext> launchContexts = new ArrayList<LaunchContext>();
    
    for (LaunchContextProto proto : launchProtos) {
      ProcessName name = new ProcessName(proto.getName());
      if (processedNames.contains(name)) {
        LOG.error("duplicated name in launch request");
        throw new HamsterException("duplicated name in launch request");
      }
      processedNames.add(name);
      
      // normalize request name
      String normalizedHostName = getNormalizedHost(proto.getHostName());
      List<HamsterContainer> containers = allocateResult.get(normalizedHostName);
      if (null == containers) {
        LOG.error("failed to get containers associated to the host:" + normalizedHostName);
        throw new HamsterException("failed to get containers associated to the host:" + normalizedHostName);
      }
      
      // find next available container in this host
      HamsterContainer container = null;
      for (HamsterContainer c : containers) {
        if (!c.isMapped()) {
          c.setName(name);
          container = c;
          break;
        }
      }
      
      // check if container is null
      if (null == container) {
        LOG.error("failed to get an available container in this host, what happened?");
        throw new HamsterException("failed to get an available container in this host, what happened?");
      }
      
      // set container-id to process name mapping, this will be useful when we get completed container from RM
      containerIdToName.put(container.getContainer().getId().getId(), name);
      
      // get envar
      Map<String, String> envarMap = new HashMap<String, String>();
      List<String> envars = proto.getEnvarsList();
      for (String env : envars) {
        int idx = env.indexOf('=');
        if (idx < 0) {
          envarMap.put(env, "");
        } else {
          envarMap.put(env.substring(0, idx), env.substring(idx + 1));
        }
      }
      
      // get args
      String args = proto.getArgs();
      
      LaunchContext ctx = new LaunchContext(envarMap, args, normalizedHostName, container.getContainer(), name);
      launchContexts.add(ctx);
    }
    
    return launchContexts.toArray(new LaunchContext[0]);
  }
  
  void doLaunch(DataOutputStream os, LaunchRequestProto launchRequest) throws IOException {
    List<LaunchContextProto> protoContexts = launchRequest.getLaunchContextsList();
    LaunchContext[] launchContexts = mapProcessToAllocatedContainers(protoContexts);
    boolean[] result = containerLauncher.launch(launchContexts);
    
    // instanity check of result
    if (result.length != launchContexts.length) {
      LOG.error("element number of launch result not equals to input element number, please check");
      throw new HamsterException("element number of launch result not equals to input element number, please check");
    }
    
    // response to hnp
    LaunchResponseProto.Builder builder = LaunchResponseProto.newBuilder();
    for (int i = 0; i < result.length; i++) {
      LaunchResultProto launchResult = LaunchResultProto.newBuilder().setSuccess(result[i]).setName(
          ProcessNameProto.newBuilder().setJobid(launchContexts[i].getName().getJobId()).
                                        setVpid(launchContexts[i].getName().getVpId()).build()).build();
      builder.addResults(launchResult);
    }
    LaunchResponseProto response = builder.build();
    
    doSuccessResponse(os, response);
  }
  
  @Override
  public void stop() {
    if (!server.isClosed()) {
      try {
        server.close();
      } catch (IOException e) {
        LOG.warn("error when close hnp service socket", e);
      }
    }
  }
  
  public int getServerPort() {
    return server.getLocalPort();
  }

}

package com.pivotal.hamster.appmaster.hnp;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.YarnException;

import com.pivotal.hamster.appmaster.allocator.ContainerAllocator;
import com.pivotal.hamster.appmaster.launcher.ContainerLauncher;
import com.pivotal.hamster.proto.HamsterProtos.HamsterHnpRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.MsgType;

public class DefaultHnpService extends HnpService {
  private static final Log LOG = LogFactory.getLog(HnpService.class);
  private static ServerSocket server;
  public static final String HANDSHAKE_MSG = "hamster-001";
  Thread serviceThread;
  ContainerAllocator containerAllocator;
  ContainerLauncher containerLauncher;
  HnpLivenessMonitor mon;
  
  public DefaultHnpService(ContainerAllocator containerAllocator, 
      ContainerLauncher containerLauncher, HnpLivenessMonitor mon) {
    super(DefaultHnpService.class.getName());
    this.containerAllocator = containerAllocator;
    this.containerLauncher = containerLauncher;
    this.mon = mon;
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
            HamsterHnpRequestProto proto = HamsterHnpRequestProto.parseFrom(is);
            if (!proto.hasMsgType() || (!proto.hasRequest())) {
              LOG.error("this proto doesn't contain msg type of request, what happened?");
              throw new IOException("this proto doesn't contain msg type of request, what happened?");
            }
            
            mon.receivedPing("hamster");
            
            MsgType type = proto.getMsgType();
            if (type == MsgType.ALLOCATE) {
              // TODO
            } else if (type == MsgType.FINISH) {
              // TODO
            } else if (type == MsgType.LAUNCH) {
              // TODO
            } else if (type == MsgType.REGISTER) {
              // TODO
            } else {
              // TODO, report error
            }
          }
          
        } catch (Exception e) {
          LOG.fatal("exception in process socket between AM and HNP", e);
          System.exit(-1);
        }
      }
      
      private void recvHandshake(InputStream in) throws Exception {
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
      
      private void sendHandshake(OutputStream out) throws Exception {
        out.write(HANDSHAKE_MSG.getBytes());
      }
      
    });
  }
  
  void doAllocate(InputStream is, OutputStream os, HamsterHnpRequestProto request) {
    
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

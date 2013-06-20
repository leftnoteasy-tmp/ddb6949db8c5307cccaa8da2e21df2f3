package com.pivotal.hamster.appmaster.ut;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProto;

import com.google.protobuf.Message;
import com.pivotal.hamster.proto.HamsterProtos.AllocateRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.FinishRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.HamsterHnpRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.LaunchRequestProto;
import com.pivotal.hamster.proto.HamsterProtos.MsgType;

public class MockRpcServer {
  public static final byte SUCCEED = 1;
  public static final byte FAILED = 2;

  static void doFailedResponse(DataOutputStream os, String msg)
      throws IOException {
    os.writeByte(FAILED);
    os.writeInt(msg.length());
    os.write(msg.getBytes());
  }

  /**
   * write success response to output pipe, consist of (1 bytes) : 1, SUCCEED (4
   * bytes) : n, length of msg n bytes, serialized msg
   * 
   * @throws IOException
   */
  static void doSuccessResponse(DataOutputStream os, Message msg) throws IOException {
    os.writeByte(SUCCEED);
    os.writeInt(msg.toByteArray().length);
    msg.writeTo(os);
  }

  public static void main(String[] args) throws Exception {
    ServerSocket server = new ServerSocket(8011);
    Socket socket = server.accept();

    // first we will recv/send handshake message
    DataInputStream is = new DataInputStream(socket.getInputStream());
    DataOutputStream os = new DataOutputStream(socket.getOutputStream());

    // we will continuously process msg
    int serializedSize = is.readInt();
    byte[] msg = new byte[serializedSize];
    int offset = 0;
    while (offset < serializedSize) {
      int rc = is.read(msg, offset, serializedSize - offset);
      if (rc < 0) {
        throw new IOException("failed to read next msg");
      }
      offset += rc;
    }
    HamsterHnpRequestProto proto = HamsterHnpRequestProto.parseFrom(msg);
    if (!proto.hasMsgType() || (!proto.hasRequest())) {
      throw new IOException(
          "this proto doesn't contain msg type of request, what happened?");
    }

    MsgType type = proto.getMsgType();
    if (type == MsgType.ALLOCATE) {
      AllocateRequestProto request = AllocateRequestProto.parseFrom(proto
          .getRequest());
      int n = request.getResourceCount();
      doSuccessResponse(os, AllocateResponseProto.newBuilder().build());
    } else if (type == MsgType.FINISH) {
      // process finish
      FinishRequestProto request = FinishRequestProto.parseFrom(proto
          .getRequest());
      boolean succeed = request.getSucceed();
      String diagnotiscMsg = request.getDiagnostics();
      doSuccessResponse(os, AllocateResponseProto.newBuilder().build());
    } else if (type == MsgType.LAUNCH) {
      // process launch
      LaunchRequestProto request = LaunchRequestProto.parseFrom(proto
          .getRequest());
    } else if (type == MsgType.REGISTER) {
      doSuccessResponse(os, AllocateResponseProto.newBuilder().build());
    } else if (type == MsgType.HEARTBEAT) {
    } else {
      doFailedResponse(os, "not a valid request type, type=" + type);
    }
    
    while (socket.isConnected()) {
      Thread.sleep(100);
    }
    is.close();
    os.close();
    socket.close();
  }
}

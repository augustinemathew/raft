package com.augustine.raft.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.8.0)",
    comments = "Source: raft.rpc.proto")
public final class RaftGrpc {

  private RaftGrpc() {}

  public static final String SERVICE_NAME = "com.augustine.raft.wal.proto.Raft";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getAppendEntriesMethod()} instead. 
  public static final io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.AppendEntriesRequest,
      com.augustine.raft.proto.RaftRpc.AppendEntriesResponse> METHOD_APPEND_ENTRIES = getAppendEntriesMethod();

  private static volatile io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.AppendEntriesRequest,
      com.augustine.raft.proto.RaftRpc.AppendEntriesResponse> getAppendEntriesMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.AppendEntriesRequest,
      com.augustine.raft.proto.RaftRpc.AppendEntriesResponse> getAppendEntriesMethod() {
    io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.AppendEntriesRequest, com.augustine.raft.proto.RaftRpc.AppendEntriesResponse> getAppendEntriesMethod;
    if ((getAppendEntriesMethod = RaftGrpc.getAppendEntriesMethod) == null) {
      synchronized (RaftGrpc.class) {
        if ((getAppendEntriesMethod = RaftGrpc.getAppendEntriesMethod) == null) {
          RaftGrpc.getAppendEntriesMethod = getAppendEntriesMethod = 
              io.grpc.MethodDescriptor.<com.augustine.raft.proto.RaftRpc.AppendEntriesRequest, com.augustine.raft.proto.RaftRpc.AppendEntriesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.augustine.raft.wal.proto.Raft", "appendEntries"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.augustine.raft.proto.RaftRpc.AppendEntriesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.augustine.raft.proto.RaftRpc.AppendEntriesResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new RaftMethodDescriptorSupplier("appendEntries"))
                  .build();
          }
        }
     }
     return getAppendEntriesMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getVoteMethod()} instead. 
  public static final io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.VoteRequest,
      com.augustine.raft.proto.RaftRpc.VoteResponse> METHOD_VOTE = getVoteMethod();

  private static volatile io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.VoteRequest,
      com.augustine.raft.proto.RaftRpc.VoteResponse> getVoteMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.VoteRequest,
      com.augustine.raft.proto.RaftRpc.VoteResponse> getVoteMethod() {
    io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.VoteRequest, com.augustine.raft.proto.RaftRpc.VoteResponse> getVoteMethod;
    if ((getVoteMethod = RaftGrpc.getVoteMethod) == null) {
      synchronized (RaftGrpc.class) {
        if ((getVoteMethod = RaftGrpc.getVoteMethod) == null) {
          RaftGrpc.getVoteMethod = getVoteMethod = 
              io.grpc.MethodDescriptor.<com.augustine.raft.proto.RaftRpc.VoteRequest, com.augustine.raft.proto.RaftRpc.VoteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.augustine.raft.wal.proto.Raft", "vote"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.augustine.raft.proto.RaftRpc.VoteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.augustine.raft.proto.RaftRpc.VoteResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new RaftMethodDescriptorSupplier("vote"))
                  .build();
          }
        }
     }
     return getVoteMethod;
  }
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  @java.lang.Deprecated // Use {@link #getInstallSnapshotMethod()} instead. 
  public static final io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest,
      com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse> METHOD_INSTALL_SNAPSHOT = getInstallSnapshotMethod();

  private static volatile io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest,
      com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse> getInstallSnapshotMethod;

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest,
      com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse> getInstallSnapshotMethod() {
    io.grpc.MethodDescriptor<com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest, com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse> getInstallSnapshotMethod;
    if ((getInstallSnapshotMethod = RaftGrpc.getInstallSnapshotMethod) == null) {
      synchronized (RaftGrpc.class) {
        if ((getInstallSnapshotMethod = RaftGrpc.getInstallSnapshotMethod) == null) {
          RaftGrpc.getInstallSnapshotMethod = getInstallSnapshotMethod = 
              io.grpc.MethodDescriptor.<com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest, com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "com.augustine.raft.wal.proto.Raft", "installSnapshot"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse.getDefaultInstance()))
                  .setSchemaDescriptor(new RaftMethodDescriptorSupplier("installSnapshot"))
                  .build();
          }
        }
     }
     return getInstallSnapshotMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static RaftStub newStub(io.grpc.Channel channel) {
    return new RaftStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static RaftBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new RaftBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static RaftFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new RaftFutureStub(channel);
  }

  /**
   */
  public static abstract class RaftImplBase implements io.grpc.BindableService {

    /**
     */
    public void appendEntries(com.augustine.raft.proto.RaftRpc.AppendEntriesRequest request,
        io.grpc.stub.StreamObserver<com.augustine.raft.proto.RaftRpc.AppendEntriesResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getAppendEntriesMethod(), responseObserver);
    }

    /**
     */
    public void vote(com.augustine.raft.proto.RaftRpc.VoteRequest request,
        io.grpc.stub.StreamObserver<com.augustine.raft.proto.RaftRpc.VoteResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getVoteMethod(), responseObserver);
    }

    /**
     */
    public void installSnapshot(com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest request,
        io.grpc.stub.StreamObserver<com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getInstallSnapshotMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getAppendEntriesMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.augustine.raft.proto.RaftRpc.AppendEntriesRequest,
                com.augustine.raft.proto.RaftRpc.AppendEntriesResponse>(
                  this, METHODID_APPEND_ENTRIES)))
          .addMethod(
            getVoteMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.augustine.raft.proto.RaftRpc.VoteRequest,
                com.augustine.raft.proto.RaftRpc.VoteResponse>(
                  this, METHODID_VOTE)))
          .addMethod(
            getInstallSnapshotMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest,
                com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse>(
                  this, METHODID_INSTALL_SNAPSHOT)))
          .build();
    }
  }

  /**
   */
  public static final class RaftStub extends io.grpc.stub.AbstractStub<RaftStub> {
    private RaftStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RaftStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RaftStub(channel, callOptions);
    }

    /**
     */
    public void appendEntries(com.augustine.raft.proto.RaftRpc.AppendEntriesRequest request,
        io.grpc.stub.StreamObserver<com.augustine.raft.proto.RaftRpc.AppendEntriesResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getAppendEntriesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void vote(com.augustine.raft.proto.RaftRpc.VoteRequest request,
        io.grpc.stub.StreamObserver<com.augustine.raft.proto.RaftRpc.VoteResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getVoteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void installSnapshot(com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest request,
        io.grpc.stub.StreamObserver<com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getInstallSnapshotMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class RaftBlockingStub extends io.grpc.stub.AbstractStub<RaftBlockingStub> {
    private RaftBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RaftBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RaftBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.augustine.raft.proto.RaftRpc.AppendEntriesResponse appendEntries(com.augustine.raft.proto.RaftRpc.AppendEntriesRequest request) {
      return blockingUnaryCall(
          getChannel(), getAppendEntriesMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.augustine.raft.proto.RaftRpc.VoteResponse vote(com.augustine.raft.proto.RaftRpc.VoteRequest request) {
      return blockingUnaryCall(
          getChannel(), getVoteMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse installSnapshot(com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest request) {
      return blockingUnaryCall(
          getChannel(), getInstallSnapshotMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class RaftFutureStub extends io.grpc.stub.AbstractStub<RaftFutureStub> {
    private RaftFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private RaftFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected RaftFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new RaftFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.augustine.raft.proto.RaftRpc.AppendEntriesResponse> appendEntries(
        com.augustine.raft.proto.RaftRpc.AppendEntriesRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getAppendEntriesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.augustine.raft.proto.RaftRpc.VoteResponse> vote(
        com.augustine.raft.proto.RaftRpc.VoteRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getVoteMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse> installSnapshot(
        com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getInstallSnapshotMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_APPEND_ENTRIES = 0;
  private static final int METHODID_VOTE = 1;
  private static final int METHODID_INSTALL_SNAPSHOT = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final RaftImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(RaftImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_APPEND_ENTRIES:
          serviceImpl.appendEntries((com.augustine.raft.proto.RaftRpc.AppendEntriesRequest) request,
              (io.grpc.stub.StreamObserver<com.augustine.raft.proto.RaftRpc.AppendEntriesResponse>) responseObserver);
          break;
        case METHODID_VOTE:
          serviceImpl.vote((com.augustine.raft.proto.RaftRpc.VoteRequest) request,
              (io.grpc.stub.StreamObserver<com.augustine.raft.proto.RaftRpc.VoteResponse>) responseObserver);
          break;
        case METHODID_INSTALL_SNAPSHOT:
          serviceImpl.installSnapshot((com.augustine.raft.proto.RaftRpc.InstallSnapshotRequest) request,
              (io.grpc.stub.StreamObserver<com.augustine.raft.proto.RaftRpc.InstallSnapshotResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class RaftBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    RaftBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.augustine.raft.proto.RaftRpc.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Raft");
    }
  }

  private static final class RaftFileDescriptorSupplier
      extends RaftBaseDescriptorSupplier {
    RaftFileDescriptorSupplier() {}
  }

  private static final class RaftMethodDescriptorSupplier
      extends RaftBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    RaftMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (RaftGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new RaftFileDescriptorSupplier())
              .addMethod(getAppendEntriesMethod())
              .addMethod(getVoteMethod())
              .addMethod(getInstallSnapshotMethod())
              .build();
        }
      }
    }
    return result;
  }
}

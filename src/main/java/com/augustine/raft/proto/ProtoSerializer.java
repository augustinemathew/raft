package com.augustine.raft.proto;

import com.google.protobuf.Message;

import java.nio.ByteBuffer;


public interface ProtoSerializer{

    <T, U extends Message>  U toProtobuf(T object);

    ByteBuffer writeToByteBuffer(Object object, ByteBuffer byteBuffer);

    <T, U extends Message>  T fromProtobuf( U protobufMesage);
}


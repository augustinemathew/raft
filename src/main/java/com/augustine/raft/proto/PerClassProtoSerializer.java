package com.augustine.raft.proto;

import com.google.protobuf.Message;

public interface PerClassProtoSerializer <T, U extends Message>{
    U toProtobuf(T object);
    T fromProtobuf(U protobufMesage);
}

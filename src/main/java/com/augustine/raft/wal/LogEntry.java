package com.augustine.raft.wal;
import com.augustine.raft.RaftConfiguration;
import com.augustine.raft.proto.ProtoSerializerImpl;
import com.augustine.raft.proto.RaftRpc;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.nio.ByteBuffer;

@Getter
@Builder(toBuilder = true)
@AllArgsConstructor
@ToString
public class LogEntry {
    private final long lsn;

    private final long term;

    @NonNull
    private final LogEntryType type;

    private final byte[] array;

    public RaftConfiguration getConfiguration() {
        Preconditions.checkState(type == LogEntryType.CONFIG);
        try {
            return RaftConfiguration.fromBytes(array);
        }catch (InvalidProtocolBufferException e){
            throw new RuntimeException(e);
        }
    }

    public ByteBuffer toByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(array.length + 40);
        byteBuffer = new ProtoSerializerImpl().writeToByteBuffer(this, byteBuffer);
        byteBuffer.flip();
        return byteBuffer;
    }

    public static class LogEntryBuilder{
        public LogEntryBuilder configuration(@NonNull RaftConfiguration configuration){
            type(LogEntryType.CONFIG);
            array(configuration.toByteArray());
            return this;
        }
       public static LogEntry fromByteBuffer(@NonNull ByteBuffer buf){
           try {
               return new ProtoSerializerImpl().fromProtobuf(RaftRpc.LogEntry.parseFrom(buf));
           }catch (InvalidProtocolBufferException ipe){
               throw new RuntimeException(ipe);
           }
       }
   }
}



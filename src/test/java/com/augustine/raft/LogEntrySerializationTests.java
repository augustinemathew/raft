package com.augustine.raft;

import com.augustine.raft.proto.ProtoSerializerImpl;
import com.augustine.raft.proto.RaftRpc;
import com.augustine.raft.wal.LogEntry;
import com.augustine.raft.wal.LogEntryType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class LogEntrySerializationTests {

    @Test
    public void testSerializeDeserializeNOOP(){
        LogEntry request = LogEntry.LogEntryBuilder.buildNoopEntry(10).toBuilder().lsn(100).build();

        RaftRpc.LogEntry protoEntry = new ProtoSerializerImpl().toProtobuf(request);
        LogEntry deserialized = new ProtoSerializerImpl().fromProtobuf(protoEntry);

        Assert.assertEquals(request, deserialized);
        Assert.assertEquals(10, request.getTerm());
        Assert.assertEquals(100, request.getLsn());
        Assert.assertEquals(request.getType(), deserialized.getType());
        Assert.assertEquals(LogEntryType.NO_OP, deserialized.getType());
    }

    @Test
    public void testSerializeDeserializeNormal(){
        LogEntry request = LogEntry.builder()
                .term(10).type(LogEntryType.NORMAL)
                .array(new byte[]{1,2,3})
                .lsn(100).build();

        RaftRpc.LogEntry protoEntry = new ProtoSerializerImpl().toProtobuf(request);
        LogEntry deserialized = new ProtoSerializerImpl().fromProtobuf(protoEntry);

        Assert.assertEquals(request, deserialized);
        Assert.assertEquals(10, request.getTerm());
        Assert.assertEquals(100, request.getLsn());
        Assert.assertArrayEquals(new byte[]{1,2,3}, request.getArray());
        Assert.assertEquals(request.getType(), deserialized.getType());
        Assert.assertEquals(LogEntryType.NORMAL, deserialized.getType());

        try{
            deserialized.getConfiguration();
            Assert.fail();
        }catch (IllegalStateException ie){

        }
    }

    @Test
    public void testSerializeDeserializeConfig(){
        RaftConfiguration raftConfig = RaftConfiguration.builder()
                .peerList(Arrays.asList(RaftPeer.builder().dnsname("foo").id(10).port(2010).build()))
                .maxElectionTimeoutInMs(5000)
                .minElectionTimeoutInMs(1000)
                .build();
        LogEntry request = LogEntry.builder()
                .term(10)
                .lsn(100)
                .configuration(raftConfig)
                .build();

        RaftRpc.LogEntry protoEntry = new ProtoSerializerImpl().toProtobuf(request);
        LogEntry deserialized = new ProtoSerializerImpl().fromProtobuf(protoEntry);

        Assert.assertEquals(request, deserialized);
        Assert.assertEquals(10, request.getTerm());
        Assert.assertEquals(100, request.getLsn());
        Assert.assertEquals(request.getConfiguration(), deserialized.getConfiguration());
        Assert.assertEquals(request.getType(), deserialized.getType());
        Assert.assertEquals(LogEntryType.CONFIG, deserialized.getType());
    }
}

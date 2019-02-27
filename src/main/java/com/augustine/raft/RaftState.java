package com.augustine.raft;

import com.augustine.raft.proto.ProtoSerializerImpl;
import com.augustine.raft.proto.RaftRpc;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.lmdbjava.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

@EqualsAndHashCode(exclude = {"stateDb", "dbEnvironment"}, doNotUseGetters = true)
public class RaftState implements AutoCloseable {
    private long currentTerm;
    private long votedFor;
    private long lastCommitedIndex;
    private long lastAppliedIndex;
    private boolean isClosed;

    //An LMDB key value pair db for storing the raft state
    private static final ByteBuffer STATE_KEY_IN_DB = (ByteBuffer) ByteBuffer.allocateDirect(4).put(new byte[]{0,1,2,3}).flip();

    private final Env<ByteBuffer> dbEnvironment;
    private final Dbi<ByteBuffer> stateDb;

    /**
     * We update the last known configuration when every we update the commit index transactionally
     * look at the method set commit index and last known configuration
     */
    private RaftConfiguration lastknownConfiguration;

    public RaftState(String filePath){
        if(!Files.exists(Paths.get(filePath))){
            new File(filePath).mkdirs();
        }
        Preconditions.checkArgument(Files.isDirectory(Paths.get(filePath)));
        // We always need an Env. An Env owns a physical on-disk storage file. One
        // Env can store many different databases (ie sorted maps).
        this.dbEnvironment = Env.create()
                // LMDB also needs to know how large our DB might be. Over-estimating is OK.
                .setMapSize(1024*1024 * 100)
                .setMaxReaders(1024)
                // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
                .setMaxDbs(1)
                // Now let's open the Env. The same path can be concurrently opened and
                // used in different processes, but do not open the same path twice in
                // the same process at the same time.
                .open(new File(filePath));
        this.stateDb = dbEnvironment.openDbi("RaftState", DbiFlags.MDB_CREATE);
        try(Txn<ByteBuffer> getTxn = this.dbEnvironment.txnRead()){
            ByteBuffer buffer = this.stateDb.get(getTxn, STATE_KEY_IN_DB);
            if(buffer != null) {
                RaftState state = fromByteBuffer(buffer);
                this.currentTerm = state.currentTerm;
                this.lastAppliedIndex = state.lastAppliedIndex;
                this.votedFor = state.votedFor;
                this.lastknownConfiguration = state.lastknownConfiguration;
                this.lastCommitedIndex = state.lastCommitedIndex;
            }else {
                this.votedFor = -1;
            }
        }
    }

    @Builder(toBuilder = true)
    RaftState(long currentTerm, long votedFor, long lastAppliedIndex, long lastCommitedIndex, RaftConfiguration lastknownConfiguration){
        this.stateDb = null;
        this.dbEnvironment = null;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastknownConfiguration = lastknownConfiguration;
        this.lastCommitedIndex = lastCommitedIndex;
    }

    public synchronized long getCurrentTerm() {
        this.throwIfClosed();
        return currentTerm;
    }

    public synchronized RaftState setCurrentTerm(long currentTerm){
        this.throwIfClosed();
        saveState(this.toBuilder().currentTerm(currentTerm).build());
        this.currentTerm = currentTerm;
        return this;
    }

    public synchronized long getLastCommitedIndex() {
        this.throwIfClosed();
        return this.lastCommitedIndex;
    }

    public synchronized RaftState setLastCommitedIndexWithConfiguration(long lastCommitedIndex,
                                                                        @NonNull RaftConfiguration lastknownConfiguration) {
        this.throwIfClosed();
        saveState(this.toBuilder()
                .lastknownConfiguration(lastknownConfiguration)
                .lastCommitedIndex(lastCommitedIndex).build());
        this.lastCommitedIndex = lastCommitedIndex;
        this.lastknownConfiguration = lastknownConfiguration;
        return this;
    }

    public RaftState setLastCommitedIndex(long lastCommitedIndex){
        this.throwIfClosed();
        saveState(this.toBuilder().lastCommitedIndex(lastCommitedIndex).build());
        this.lastCommitedIndex = lastCommitedIndex;
        return this;
    }

    public synchronized long getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    public synchronized RaftState setLastAppliedIndex(long lastAppliedIndex) {
        this.throwIfClosed();
        saveState(this.toBuilder().lastAppliedIndex(lastAppliedIndex).build());
        this.lastAppliedIndex = lastAppliedIndex;
        return this;
    }

    public synchronized long getVotedFor(){
        this.throwIfClosed();
        return this.votedFor;
    }

    public synchronized RaftState setVotedFor(long votedFor) {
        this.throwIfClosed();
        Preconditions.checkArgument(votedFor == -1 || lastknownConfiguration.getPeers().containsKey(votedFor));
        saveState(this.toBuilder().votedFor(votedFor).build());
        this.votedFor = votedFor;
        return this;
    }

    public synchronized RaftState setVotedForAndCurrentTerm(long votedFor, long currentTerm){
        this.throwIfClosed();
        Preconditions.checkArgument(votedFor == -1 || lastknownConfiguration.getPeers().containsKey(votedFor));
        saveState(this.toBuilder().votedFor(votedFor).currentTerm(currentTerm).build());
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        return this;
    }

    public synchronized RaftConfiguration getLastKnownGoodConfiguration(){
        return this.lastknownConfiguration;
    }

    public synchronized RaftState setLastKnownGoodConfiguration(long lastAppliedIndex, @NonNull RaftConfiguration raftConfiguration) {
        this.throwIfClosed();
        saveState(this.toBuilder().lastknownConfiguration(raftConfiguration).build());
        this.lastknownConfiguration = raftConfiguration;
        return this;
    }

    private synchronized void saveState(RaftState state){
        Preconditions.checkState(this.stateDb != null);
        try(Txn txn = dbEnvironment.txnWrite()) {
            this.stateDb.put(txn,STATE_KEY_IN_DB, toByteBuffer(state));
            txn.commit();
        }
    }

    @Override
    public synchronized void close() {
        if(!this.isClosed && this.stateDb != null) {
            this.stateDb.close();
            this.dbEnvironment.close();
        }
        this.isClosed = true;
    }

    private void throwIfClosed(){
        Preconditions.checkState(!this.isClosed, "This instance is closed");
    }

    private static ByteBuffer toByteBuffer(RaftState state){
        byte[] array = new ProtoSerializerImpl().toProtobuf(state).toByteArray();
        ByteBuffer buffer= ByteBuffer.allocateDirect(array.length);
        buffer.put(array);
        buffer.flip();
        return buffer;
    }

    private static RaftState fromByteBuffer(ByteBuffer byteBuffer){
        try {
            return new ProtoSerializerImpl().fromProtobuf(RaftRpc.PersistentState.parseFrom(byteBuffer));
        }catch (InvalidProtocolBufferException ipe){
            throw new RuntimeException(ipe);
        }
    }
}

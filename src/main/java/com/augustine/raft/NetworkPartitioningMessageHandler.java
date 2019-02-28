package com.augustine.raft;

import com.augustine.raft.rpc.*;
import lombok.NonNull;

import java.util.concurrent.ConcurrentSkipListSet;

public class NetworkPartitioningMessageHandler extends DelegatingRaftMessageHandler {
    private final ConcurrentSkipListSet<Long> partitionedServers = new ConcurrentSkipListSet<>();

    NetworkPartitioningMessageHandler(@NonNull RaftMessageHandler raftMessageHandler) {
        super(raftMessageHandler);
    }

    public void setPartition(long... serverIds){
        for(long serverId : serverIds){
            setPartition(serverId);
        }
    }

    public boolean setPartition(long serverId){
       return this.partitionedServers.add(serverId);
    }

    public void healAllPartitions(){
        partitionedServers.clear();
    }

    public boolean removePartition(long serverId){
        return this.partitionedServers.remove(serverId);
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        if(this.partitionedServers.contains(request.getServerId())) {
            throw new IllegalStateException("Simulating network partition");
        }
        return super.handleAppendEntries(request);
    }

    @Override
    public VoteResponse handleVoteRequest(VoteRequest request) {
        if(this.partitionedServers.contains(request.getServerId())) {
            throw new IllegalStateException("Simulating network partition");
        }
        return super.handleVoteRequest(request);
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        if(this.partitionedServers.contains(request.getServerId())) {
            throw new IllegalStateException("Simulating network partition");
        }
        return super.handleInstallSnapshot(request);
    }
}

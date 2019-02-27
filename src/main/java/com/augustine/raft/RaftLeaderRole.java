package com.augustine.raft;

import com.augustine.raft.rpc.AppendEntriesRequest;
import com.augustine.raft.rpc.AppendEntriesResponse;
import com.augustine.raft.wal.LogEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public class RaftLeaderRole extends RaftRole {
    private final AtomicBoolean cancelLeaderHeartbeat = new AtomicBoolean(false);
    @Getter(AccessLevel.PACKAGE)
    private volatile long[] nextIndex;
    @Getter(AccessLevel.PACKAGE)
    private volatile long[] matchIndex;
    private volatile ScheduledFuture leaderHeartbeatTask;

    public RaftLeaderRole(@NonNull RaftServer server) {
        super(server);
        initializeNextAndMatchIndices();
    }

    public boolean isRunning(){
        return this.leaderHeartbeatTask != null;
    }

    public synchronized void start(){
        if(leaderHeartbeatTask != null) {
            return;
        }
        this.cancelLeaderHeartbeat.set(false);
        this.initializeNextAndMatchIndices();
        this.leaderHeartbeatTask = this.server.getScheduledExecutorService()
                .schedule(this::leaderHeartbeatTask,0, TimeUnit.MILLISECONDS);
        info("Started leader heartbeat task");
    }

    public synchronized void stop() {
        if(!this.isRunning()) {
            return;
        }

        this.cancelLeaderHeartbeat.set(true);
        try{
            this.leaderHeartbeatTask.get();
        }catch (InterruptedException | ExecutionException ignored) {

        }
        info("Stopping leader heartbeat task");
        this.leaderHeartbeatTask = null;
    }

    private void initializeNextAndMatchIndices() {
        int peerCount = this.getRaftConfiguration().getPeerList().size();
        nextIndex = new long[peerCount];
        matchIndex = new long[peerCount];
        long lastEntryIndex = this.getWriteAheadLog().getLastEntryIndex();
        for(int i=0; i< this.nextIndex.length; i++){
            nextIndex[i] = lastEntryIndex +1;
            matchIndex[i] = 0;
        }

        //Let up update our next index and match index to
        this.nextIndex[(int)this.getServerId()] = this.getWriteAheadLog().getLastEntryIndex() + 1;
        this.matchIndex[(int)this.getServerId()] = this.getWriteAheadLog().getLastEntryIndex();
    }

    private void leaderHeartbeatTask(){
        while (!this.cancelLeaderHeartbeat.get()) {
            try {
                info("sending append entries");
                Map<RaftPeer, AppendEntriesRequest> peerToRequestMap = new HashMap<>();
                Map<RaftPeer,CompletableFuture<AppendEntriesResponse>> peerToResponseMap =
                        new HashMap<>();
                for (RaftPeer peer : this.getRaftConfiguration().getPeerList()) {
                    if(peer.getId() == this.getServerId())
                        continue;
                    AppendEntriesRequest request = this.getAppendEntriesRequestForPeer((int)peer.getId());
                    peerToRequestMap.put(peer,request);
                    peerToResponseMap.put(peer, this.server.getClient(peer.getId()).AppendEntries(request).exceptionally((e) ->{
                        error("Exception occurred while sending append entries {}", e);
                        return null;
                    }));
                }

                waitUntilResponsesRecvd(peerToResponseMap);

                for (RaftPeer peer : peerToResponseMap.keySet().stream().filter(l -> peerToResponseMap.get(l).isDone())
                        .collect(Collectors.toList())) {
                    AppendEntriesResponse response = peerToResponseMap.get(peer).get();
                    if (response != null ) {
                        if(this.server.updateTermIfWeAreBehindAndTryConvertToFollower(response.getTerm())){
                            return;
                        }
                        updateMatchIndexForPeer(peer, peerToRequestMap.get(peer), response);
                    }
                }
                CompletableFutures.cancelAll(peerToResponseMap.values(), true);
                this.server.setLastCommitIndex(computeCommitIndex());
            } catch (ExecutionException e) {
                error("error in heart beat task {}", e);
            } catch (InterruptedException e) {
                break;
            } catch (Exception  e){
                error(" error in heart beat task {}",e);
            }

            Uninterruptibles.sleepUninterruptibly(
                    this.getRaftConfiguration().getLeaderHeartbeatIntervalInMs(),
                    TimeUnit.MILLISECONDS);
        }
        info("Exiting leader heartbeat task");
    }

    private void waitUntilResponsesRecvd(Map<RaftPeer, CompletableFuture<AppendEntriesResponse>> peerToResponseMap)
            throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        try {
            CompletableFuture.allOf(peerToResponseMap.values()
                    .toArray(new CompletableFuture[peerToResponseMap.size()]))
                    .get(this.getRaftConfiguration().getLeaderHeartbeatIntervalInMs(), TimeUnit.MILLISECONDS);
        }catch (Exception e){
            error("An exception occurred while waiting for appendEntries responses");
            for(RaftPeer peer : peerToResponseMap.keySet()){
                CompletableFuture responseFuture = peerToResponseMap.get(peer);
                if(responseFuture.isCompletedExceptionally()) {
                    error("Exception occurred for peer network call {}", peer);
                }
            }
        }
    }

    @VisibleForTesting
    synchronized AppendEntriesRequest getAppendEntriesRequestForPeer(int peerId) {
        AppendEntriesRequest.AppendEntriesRequestBuilder request =
                AppendEntriesRequest.builder();
        request.leaderId(this.getServerId())
                .serverId(this.getServerId())
                .leaderCommit(this.server.getLastCommitIndex());

        long nextIndex = this.nextIndex[peerId];
        info("append request for peer {} nextIndex {} lastEntryIndex {}" , peerId, nextIndex,
                this.getWriteAheadLog().getLastEntryIndex());
        request.prevLogIndex(nextIndex - 1);
        request.prevLogTerm(this.getWriteAheadLog().getLogEntry(nextIndex - 1).getTerm());
        request.term(this.server.getServerState().getCurrentTerm());
        info("append request for peer {} {}",  peerId, request);
        if(nextIndex<=this.getWriteAheadLog().getLastEntryIndex()) {
            List<LogEntry> entries = this.getWriteAheadLog().getLogEntries(nextIndex, Math.min(this.getWriteAheadLog().getLastEntryIndex(),
                    nextIndex + 10000));
            request.entries(entries.toArray(new LogEntry[entries.size()]));
        } else {
            request.entries(new LogEntry[]{});
        }
        return request.build();
    }

    @VisibleForTesting
    synchronized long computeCommitIndex() {
        long[] sortedMatch = Arrays.stream(matchIndex).sorted().toArray();
        long newCommitIndex = Math.max(sortedMatch[server.getMajority()], server.getLastCommitIndex());
        return newCommitIndex;
    }

    @VisibleForTesting
    synchronized void updateMatchIndexForPeer(RaftPeer peer,
                                              AppendEntriesRequest request,
                                              AppendEntriesResponse response) {

        info("Updating matchIndex for peer {} indexOnServer {}",  peer.getId(), response);
        int peerId = (int) peer.getId();
        /**
         * So if we succeeded lets update the next index to next position in our log
         */
        if (response.isSucceeded()) {
            this.nextIndex[peerId] = request.getPrevLogIndex() + Math.max(request.getEntries().length, 1);
            this.matchIndex[peerId] = this.nextIndex[peerId]  - 1;
        } else {
            /**
             * Failure case is tricky we can fail due to conflicting log entries.
             * In that case we can back track until the beginning of conflicting term and lsn
             * Otherwise if we failed because the log on the other server is far behind we can set our next index
             * to that lsn
             */
            if (response.getConflictDetails() != null) {
                this.nextIndex[peerId] = response.getConflictDetails().getLogIndexOfFirstEntryInConflictingTerm();
            } else {
                if(nextIndex[peerId] > response.getLastLogIndexOnServer()) {
                    this.nextIndex[peerId] = response.getLastLogIndexOnServer() + 1;
                }else {
                    this.nextIndex[peerId]--;
                }
            }
            //Finally when we do this computation we want to make sure that we don't blow past the log on our
            //server for next index calculation.
            if(this.nextIndex[peerId] - 1 > this.getWriteAheadLog().getLastEntryIndex()) {
                this.nextIndex[peerId] = this.getWriteAheadLog().getLastEntryIndex() + 1;
            }
            //When we fail we don't know how far the log is up to date so let us assume the worst.
            this.matchIndex[peerId] = 0;
        }

        info("Updated next index for peer {} to {} match = {} (lastLogIndex {}) succeeded {}",  peer.getId(),
                this.nextIndex[peerId] ,
                this.matchIndex[peerId],
                this.getWriteAheadLog().getLastEntryIndex(),
                response.isSucceeded());
    }
}
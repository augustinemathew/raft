package com.augustine.raft;

import com.augustine.raft.proto.ProtoSerializerImpl;
import com.augustine.raft.rpc.AppendEntriesRequest;
import com.augustine.raft.rpc.AppendEntriesResponse;
import com.augustine.raft.rpc.InstallSnapshotRequest;
import com.augustine.raft.rpc.InstallSnapshotResponse;
import com.augustine.raft.rpc.RaftRpcClient;
import com.augustine.raft.rpc.VoteRequest;
import com.augustine.raft.rpc.VoteResponse;
import com.augustine.raft.rpc.impl.GrpcRaftServer;
import com.augustine.raft.snapshot.Snapshot;
import com.augustine.raft.snapshot.SnapshotManager;
import com.augustine.raft.wal.Log;
import com.augustine.raft.wal.LogEntry;
import com.augustine.raft.wal.LogEntryType;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


public class RaftServer implements RaftMessageHandler {

    @Getter
    private final ServerConfiguration serverConfig;

    @Getter(AccessLevel.PACKAGE)
    private final RaftState serverState;
    private final Thread stateMachineThread;
    private final StateMachine stateMachine;

    @Getter(AccessLevel.PACKAGE)
    private final ScheduledExecutorService scheduledExecutorService;
    @Getter
    private final long serverId;

    private final Logger log;

    private ScheduledFuture electionTimeoutTask;

    private volatile ServerRole serverRole;

    private final AtomicBoolean stopStateMachineApplicationThread;

    private final ConcurrentHashMap<Long,RaftRpcClient> rpcClients = new ConcurrentHashMap<>();

    private volatile long currentLeaderId;

    private final GrpcRaftServer server;

    private volatile boolean isRunning;
    private SnapshotManager snapshotManager;
    private volatile Instant lastAppendEntriesOrVoteGranted;


    private final RaftLeaderRole leaderState;
    private final RaftCandidateRole candidateState;

    public RaftServer(ServerConfiguration serverConfig,
                      StateMachine stateMachine) {
        this.log = LoggerFactory.getLogger(this.getClass().getName());
        this.serverConfig = serverConfig;
        this.serverState = serverConfig.getRaftState();
        this.stateMachine = stateMachine;

        this.serverId = this.serverConfig.getServerId();
        this.serverRole = ServerRole.Follower;
        this.stopStateMachineApplicationThread = new AtomicBoolean(false);
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(10);
        this.server = new GrpcRaftServer(this.getRaftConfiguration()
                .getPeerList().get((int)this.serverId).getPort(), this, new ProtoSerializerImpl());
        this.stateMachineThread = new Thread(createStateMachineApplicationTask());
        this.stateMachineThread.setDaemon(true);
        this.lastAppendEntriesOrVoteGranted = Instant.MIN;
        this.currentLeaderId = -1;
        this.leaderState = new RaftLeaderRole(this);
        this.candidateState = new RaftCandidateRole(this);
    }

    private Log getWriteAheadLog() {
        return this.getServerConfig().getWriteAheadLog();
    }

    private Runnable createStateMachineApplicationTask(){
        return () -> {
            RaftServer raftServer = RaftServer.this;

            while (!raftServer.stopStateMachineApplicationThread.get() && !Thread.interrupted()){
                if(raftServer.serverState.getLastAppliedIndex()<raftServer.serverState.getLastCommitedIndex()){
                    long nextIndexToApply = raftServer.serverState.getLastAppliedIndex() + 1;
                    raftServer.stateMachine.apply(nextIndexToApply,this.getWriteAheadLog().getLogEntry(nextIndexToApply).getArray());
                    serverState.setLastAppliedIndex(nextIndexToApply);
                }else {
                    try {
                        Thread.sleep(1);
                    }catch (InterruptedException iex){
                        this.log.warn("State machine thread interrupted. Exiting");
                        return;
                    }
                }
            }
            this.log.info("Exiting state machine application thread");
        };
    }

    public synchronized void start() throws IOException{
        if(!this.isRunning) {
            startStateMachineThread();
            this.server.start();
            this.convertToFollower();
            this.isRunning = true;
        }
    }

    private synchronized void startStateMachineThread(){
        if(this.stateMachineThread.isAlive()) {
            stopStateMachineThread();
        }
        this.stopStateMachineApplicationThread.set(false);
        this.stateMachineThread.start();
    }

    private synchronized void stopStateMachineThread(){
        if(this.stateMachineThread.isAlive()){
            this.stopStateMachineApplicationThread.set(true);
            try {
                this.stateMachineThread.join();
            }catch (InterruptedException ie){
                throw new RuntimeException(ie);
            }
        }
    }

    synchronized void convertToFollower(){
        //We can get called this from two states
        //We are either a candidate or a leader
        //In this function we need to kill extraneous stuff.
        if(this.serverRole == ServerRole.Candidate){
            this.candidateState.cancelOngoingElection();
        }else if(this.serverRole == ServerRole.Leader){
            this.leaderState.stop();
        }
        this.serverRole = ServerRole.Follower;
        resetElectionTimer();
    }

    public ServerRole getServerRole() {
        return serverRole;
    }

    public synchronized void proposeConfigurationChange(RaftConfiguration raftConfiguration){
        if(this.serverRole != ServerRole.Leader){
            throw new IllegalArgumentException("Not leader");
        }

        Optional<LogEntry> lastUncommittedConfigEntry = this.getWriteAheadLog()
                .getLogEntries(this.serverState.getLastCommitedIndex()).stream()
                .filter(f -> f.getType() == LogEntryType.CONFIG).findAny();
        if(lastUncommittedConfigEntry.isPresent()) {
            throw new IllegalStateException("There is already an uncommitted configuration entry");
        }

        this.getWriteAheadLog().appendLogEntries(Arrays.asList(LogEntry.builder()
                .type(LogEntryType.CONFIG)
                .configuration(raftConfiguration)
                .term(this.serverState.getCurrentTerm())
                .build()));
    }

    public synchronized void propose(@NonNull List<byte[]> proposals){
        if(this.serverRole != ServerRole.Leader){
            throw new IllegalArgumentException("Not leader");
        }

        this.log.info("{} proposed {} entries for term = {}",this, proposals.size(),
                this.serverState.getCurrentTerm());
        this.getWriteAheadLog().appendLogEntries(proposals.stream().map(p ->
        LogEntry.builder()
                .type(LogEntryType.NORMAL)
                .term(this.serverState.getCurrentTerm()).array(p).build())
                .collect(Collectors.toList()));
    }

    synchronized void convertToCandidate(){
        if(this.serverRole == ServerRole.Leader){
            this.log.info("{} Converting to candidate. Cancelling heartbeat task.", this.toString());
            this.leaderState.stop();
        }else if(this.serverRole == ServerRole.Candidate){
            this.log.error("{} We are trying to be a candidate while in the candidate state.", this.toString());
            throw new RuntimeException("Should not occur");
        }

        this.serverRole = ServerRole.Candidate;
        resetElectionTimer();
        this.candidateState.performElection();
    }


    /**
     * Get quorum size of the cluster
     * @return an integer representing quorum size
     */
    int getMajority() {
        return (this.getRaftConfiguration().getPeerList().size())/2 + 1;
    }

    private RaftConfiguration getRaftConfiguration() {
        return this.serverState.getLastKnownGoodConfiguration();
    }

    public void tryConvertToLeader() {
        this.scheduledExecutorService.schedule(this::convertToLeader,0, TimeUnit.MILLISECONDS);
    }

    private synchronized void convertToLeader(){
        if(this.serverRole != ServerRole.Candidate){
            throw new RuntimeException("Invalid state should not occur");
        }

        cancelElectionTimer();
        this.candidateState.cancelOngoingElection();
        this.serverRole = ServerRole.Leader;
        this.getWriteAheadLog().appendLogEntries(Arrays.asList(
                LogEntry.builder().term(this.serverState.getCurrentTerm())
                                .type(LogEntryType.NORMAL)
                                .array(new byte[0])
                                .build()));

        this.leaderState.start();
        this.currentLeaderId = this.serverId;
    }

    private synchronized void electionTimeoutExpired(){
        log.info("{} Election timer fired {}", this, this.serverRole.toString());
        if(this.serverRole == ServerRole.Follower){
            /*
             Rules for followers:
                If election timeout elapses without receiving AppendEntries
                RPC from current leader or granting vote to candidate:
                convert to candidate.
            */
            Duration elaspedRpcTime = Duration.between(lastAppendEntriesOrVoteGranted, Instant.now());
            if(//Check if we revd append entries
                (elaspedRpcTime.compareTo(Duration.ofMillis(this.getRaftConfiguration().getMinElectionTimeoutInMs())) <=0)) {
                resetElectionTimer();
                return;
            }
            this.log.info("{} Election timer expired converting to candidate elapsed time {}", this, elaspedRpcTime);
            this.convertToCandidate();
        }else if(this.serverRole == ServerRole.Candidate){
            this.candidateState.performElection();
            resetElectionTimer();
        }else if(this.serverRole == ServerRole.Leader){
            this.log.warn("{} should occur as election timeout should only be triggered on candidate or follower");
        }
    }

    private synchronized void resetElectionTimer(){
        cancelElectionTimer();
        final long electionTimeoutInMs = this.getRaftConfiguration().getRandomizedElectionTimeout();
        this.log.info("{} Setting election timeout to {} ms", this, electionTimeoutInMs);
        this.electionTimeoutTask = this.scheduledExecutorService.schedule(this::electionTimeoutExpired,
                                                                                     electionTimeoutInMs,TimeUnit.MILLISECONDS);
    }

    private synchronized void cancelElectionTimer(){
        if(this.electionTimeoutTask != null) {
            this.electionTimeoutTask.cancel(true);
            try {
                this.electionTimeoutTask.get();
            }catch (InterruptedException|CancellationException|ExecutionException e){
                //this.log.error(this + " election timer cancellation ", e);
            }
            this.electionTimeoutTask = null;
        }
    }

    synchronized boolean updateTermIfWeAreBehindAndTryConvertToFollower(long term) {
        if(this.serverState.getCurrentTerm() < term) {
            this.scheduledExecutorService.execute(()->this.updateTermIfWeAreBehindAndTryConvertToFollower(term));
            return true;
        }
        return false;
    }

    private synchronized boolean updateTermIfWeAreBehindAndConvertToFollower(long term){
        long oldTerm = this.serverState.getCurrentTerm();
        if(updateTerm(term)){
            this.log.info("{} Converting to follower due to term mismatch new term {} {}", this, oldTerm, term);
            this.convertToFollower();
            return true;
        }else{
            return false;
        }
    }

    private synchronized boolean updateTerm(long term){
        if(this.serverState.getCurrentTerm() < term) {
            this.log.info("{} Term {} is > our term {}",this, term, this.serverState.getCurrentTerm());
            this.serverState.setVotedForAndCurrentTerm(-1,term);
            return true;
        }else{
            return false;
        }
    }

    @Override
    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        this.lastAppendEntriesOrVoteGranted = Instant.now();

        if(this.updateTermIfWeAreBehindAndConvertToFollower(request.getTerm())){
           this.log.info("Append entries term is > our term ");
        }

        if(this.currentLeaderId != request.getLeaderId()) {
            this.currentLeaderId = request.getLeaderId();
        }


        this.log.info("{} Current log status lastIndex {} lastTerm {} request prev Index {} prev Term {} entry count {}",this,
                this.getWriteAheadLog().getLastEntryIndex(), this.getWriteAheadLog()
                        .getLogEntry(this.getWriteAheadLog().getLastEntryIndex()).getTerm(),
                request.getPrevLogIndex(), request.getPrevLogTerm(), request.getEntries().length);
        AppendEntriesResponse.AppendEntriesResponseBuilder responseBuilder = AppendEntriesResponse.builder();
        responseBuilder.term(this.serverState.getCurrentTerm());
        if(request.getTerm() < this.serverState.getCurrentTerm()) {
            responseBuilder.succeeded(false);
            responseBuilder.lastLogIndexOnServer(this.getWriteAheadLog().getLastEntryIndex());
            return responseBuilder.build();
        }

        boolean logContainsEntryAtPrevIndex = this.getWriteAheadLog().getLastEntryIndex() >= request.getPrevLogIndex();
        if(!logContainsEntryAtPrevIndex){
            responseBuilder.succeeded(false);
            responseBuilder.lastLogIndexOnServer(this.getWriteAheadLog().getLastEntryIndex());
            return responseBuilder.build();
        }

        boolean termsMatchForLogEntryAtPrevIndex = this.getWriteAheadLog().getLogEntry(request.getPrevLogIndex()).getTerm()
                                                    == request.getPrevLogTerm();
        if(!termsMatchForLogEntryAtPrevIndex){
            responseBuilder.term(this.serverState.getCurrentTerm());
            responseBuilder.succeeded(false);
            long conflictingTerm = this.getWriteAheadLog().getLogEntry(request.getPrevLogIndex()).getTerm();
            long firstEntryForConflictingTerm = getFirstIndexOfTermInLog(conflictingTerm);
            responseBuilder.conflictDetails(AppendEntriesResponse.ConflictDetails.builder()
                                            .conflictingTerm(conflictingTerm)
                                            .logIndexOfFirstEntryInConflictingTerm(firstEntryForConflictingTerm)
                                            .build());
            return responseBuilder.build();
        }else{
            responseBuilder.term(this.serverState.getCurrentTerm());
            responseBuilder.succeeded(true);
        }

        /*
         If an existing entry conflicts with a new one (same index
         but different terms), delete the existing entry and all that
         follow it (§5.3)
         Append any entries that raftlog doesn't contain
        */
        long nextLogIndex = request.getPrevLogIndex()+1;
        int entriesToSkip = 0;

        //Check for conflict or look for entries that match to skip
        boolean didnotConflict = true;
        for (;entriesToSkip<request.getEntries().length && nextLogIndex <= this.getWriteAheadLog().getLastEntryIndex() &&
                (didnotConflict = this.getWriteAheadLog().getLogEntry(nextLogIndex).getTerm() ==
                        request.getEntries()[entriesToSkip].getTerm());
                entriesToSkip++,nextLogIndex++);
        //If a conflict is found remove all entries proceeding it including that entry
        if(nextLogIndex<= this.getWriteAheadLog().getLastEntryIndex() && !didnotConflict){
            this.log.warn("{} found conflicting entries at {}", this, nextLogIndex);
            this.getWriteAheadLog().removeEntriesStartingFromIndex(nextLogIndex);
        }

        //Append the remaining entries to the end of the raftlog if any
        if(entriesToSkip<request.getEntries().length){
            if(entriesToSkip + request.getPrevLogIndex() != this.getWriteAheadLog().getLastEntryIndex()){
                this.log.error("{} CRAY CRAY Corruption", this);
            }
            this.log.info("Appending entries starting from {} upto including {}",entriesToSkip, request.getEntries().length-1);
            this.getWriteAheadLog().appendLogEntries(Arrays.asList(request.getEntries()).subList(entriesToSkip, request.getEntries().length));
        }

        setLastCommitIndex(request.getLeaderCommit());

        responseBuilder.lastLogIndexOnServer(this.getWriteAheadLog().getLastEntryIndex());
        return responseBuilder.build();
    }

    private long getFirstIndexOfTermInLog(long term) {
        for(long i=this.getWriteAheadLog().getFirstEntryIndex(); i<=this.getWriteAheadLog().getLastEntryIndex();i+=1000){
            List<LogEntry> entries = this.getWriteAheadLog().getLogEntries(i, Math.min(i+1000, this.getWriteAheadLog().getLastEntryIndex()));
            for(LogEntry entry : entries){
                if(entry.getTerm() == term) {
                    return entry.getLsn();
                }
            }
        }
        return -1;
    }

    @Override
    public synchronized VoteResponse handleVoteRequest(VoteRequest request) {
        if(this.updateTermIfWeAreBehindAndConvertToFollower(request.getTerm())){
            this.log.info("{} Recvd vote request.",this);
        }

        VoteResponse.VoteResponseBuilder responseBuilder = VoteResponse.builder();
        responseBuilder.term(this.serverState.getCurrentTerm());
        if(request.getTerm() < this.serverState.getCurrentTerm()){
            this.log.info("{} Vote request for {}: request term {} < current term {}. Not granting vote", this,
                    request.getServerId(),request.getTerm(),this.serverState.getCurrentTerm());
            responseBuilder.voteGranted(false);
            return responseBuilder.build();
        }
        /*
        If votedFor is null or candidateId, and candidate’s raftlog is at
        least as up-to-date as receiver’s raftlog, grant vote (§5.2, §5.4)
        */
        long logIndex = this.getWriteAheadLog().getLastEntryIndex();
        long logTerm = this.getWriteAheadLog().getLogEntry(logIndex).getTerm();

        boolean canVoteForCandidate = this.serverState.getVotedFor() == -1 ||
                this.serverState.getVotedFor() == request.getCandidateId();

        boolean ourLogIsAtleastAsUpdateAsCandidate =(request.getLastLogTerm()>logTerm ||
                (request.getLastLogTerm() == logTerm && logIndex<= request.getLastLogIndex()));

        if(canVoteForCandidate && ourLogIsAtleastAsUpdateAsCandidate) {
            this.log.info("{} Granting vote for {}", this, request.getCandidateId());
            this.serverState.setVotedFor(serverId);
            this.lastAppendEntriesOrVoteGranted = Instant.now();
            responseBuilder.voteGranted(true);
        }else{
            this.log.info("{} Rejecting vote for {} CanVote: {} logIsGood: {}", this, request.getCandidateId(), canVoteForCandidate, ourLogIsAtleastAsUpdateAsCandidate);
            responseBuilder.voteGranted(false);
        }
        return responseBuilder.build();
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {

        InstallSnapshotResponse.InstallSnapshotResponseBuilder builder = InstallSnapshotResponse.builder();
        builder.term(this.serverState.getCurrentTerm());
        if(updateTermIfWeAreBehindAndConvertToFollower(request.getTerm())){
            return builder.ok(false)
                    .build();
        }

        resetElectionTimer();

        try {
            if(!request.isDone()) {
                this.snapshotManager.appendToSnapshot(request.getLastIncludedIndex(), request.getLastIncludedTerm(), request.getOffset(),
                        request.getData());
                return builder.ok(true).build();
            }else{

                Snapshot snapshot = this.snapshotManager.saveSnapshot(
                        request.getLastIncludedIndex(),
                        request.getLastIncludedTerm(),
                        request.getConfiguration(),
                        request.getSnaphotCheckum());
                return builder.ok(true).build();
            }
        }catch (IOException ioe){
            this.log.error("{} error in storing snapshot", this,ioe);
            return builder.ok(false).build();
        }
    }

    synchronized long setLastCommitIndex(long lastCommitIndex){
        if(lastCommitIndex > this.serverState.getLastCommitedIndex()) {
            List<LogEntry> logEntries = this.getWriteAheadLog()
                    .getLogEntries(this.serverState.getLastCommitedIndex(), lastCommitIndex);
            LogEntry newConfiguration = null;
            for(LogEntry entry : logEntries) {
                if(entry.getType() == LogEntryType.CONFIG) {
                    newConfiguration = entry;
                }
            }
            if(newConfiguration != null) {
                this.serverState.setLastCommitedIndexWithConfiguration(lastCommitIndex,
                        newConfiguration.getConfiguration());
            }else {
                this.serverState.setLastCommitedIndex(lastCommitIndex);
            }
        }
        return this.serverState.getLastCommitedIndex();
    }

    synchronized long getLastCommitIndex(){
        return this.serverState.getLastCommitedIndex();
    }

    RaftRpcClient getClient(long peerId){
        Preconditions.checkArgument(this.getRaftConfiguration().getPeers().containsKey(peerId));
        return this.rpcClients
                .computeIfAbsent(peerId,
                        id -> this.getRaftConfiguration().getPeerList().get((int)peerId)
                                .getClient(Math.min(10,this.getRaftConfiguration().getMinElectionTimeoutInMs()/2)));
    }

    public String toString(){
        return "ID " + this.serverId;
    }
}

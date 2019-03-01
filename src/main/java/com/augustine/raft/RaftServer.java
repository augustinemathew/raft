package com.augustine.raft;

import com.augustine.raft.proto.ProtoSerializerImpl;
import com.augustine.raft.rpc.*;
import com.augustine.raft.rpc.impl.GrpcRaftServer;
import com.augustine.raft.snapshot.SnapshotManager;
import com.augustine.raft.wal.Log;
import com.augustine.raft.wal.LogEntry;
import com.augustine.raft.wal.LogEntryType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
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

    public interface RaftServerEventListener extends Comparable<RaftServerEventListener> {
        default void onLeaderChange(long newLeader, long newLeaderTerm){}
        default void onServerStateRole(ServerRole current, ServerRole next, long currentTerm){}

        @Override
        default int compareTo(RaftServerEventListener o) {
            if(o == null){
                return 1;
            }

            return Integer.compare(this.hashCode(), o.hashCode());
        }
    }

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

    private volatile ServerRole serverRole;

    private final AtomicBoolean stopStateMachineApplicationThread;

    private final ConcurrentHashMap<Long,RaftRpcClient> rpcClients = new ConcurrentHashMap<>();
    private final Map<ServerRole, RaftRole> serverRoleRaftRoleMap;


    private final GrpcRaftServer server;

    private volatile boolean isRunning;
    private SnapshotManager snapshotManager;
    private volatile Instant lastAppendEntriesOrVoteGrantedOrMajorityResponse;
    @Getter
    private final RaftMessageHandler currentMessageHandler;

    private final Thread timerThread;
    private final AtomicBoolean stopTimerThread;
    private volatile long currentLeaderId;
    private volatile long currentLeaderTerm;

    private final ConcurrentSkipListSet<RaftServerEventListener> eventListener = new ConcurrentSkipListSet<>();

    public RaftServer(@NonNull ServerConfiguration serverConfig,
                      @NonNull StateMachine stateMachine){
        this(serverConfig, stateMachine, Functions.identity());
    }

    public RaftServer(@NonNull ServerConfiguration serverConfig,
                      @NonNull StateMachine stateMachine,
                      @NonNull Function<RaftMessageHandler, RaftMessageHandler> handlerTransformer) {
        this.log = LoggerFactory.getLogger(this.getClass().getName());
        this.serverConfig = serverConfig;
        this.serverState = serverConfig.getRaftState();
        this.stateMachine = stateMachine;

        this.serverId = this.serverConfig.getServerId();
        this.serverRole = ServerRole.Follower;
        this.stopStateMachineApplicationThread = new AtomicBoolean(false);
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(10);
        this.server = new GrpcRaftServer(
                this.getRaftConfiguration().getPeerList()
                        .get((int)this.serverId).getPort(),
                this.currentMessageHandler = handlerTransformer.apply(this),
                new ProtoSerializerImpl());
        this.stateMachineThread = new Thread(createStateMachineApplicationTask());
        this.stateMachineThread.setDaemon(true);
        this.stateMachineThread.setName("StateMachine-Server-"+ this.serverId);
        this.timerThread = new Thread(this::timerThreadLogic);
        this.timerThread.setDaemon(true);
        this.timerThread.setName("Timer-Server-"+ this.serverId);
        this.stopTimerThread = new AtomicBoolean(true);
        this.lastAppendEntriesOrVoteGrantedOrMajorityResponse = Instant.MIN;
        this.currentLeaderId = -1;
        this.serverRoleRaftRoleMap = ImmutableMap.of(ServerRole.Leader, new RaftLeaderRole(this),
                                                     ServerRole.Candidate, new RaftCandidateRole(this),
                                                     ServerRole.Follower, new RaftFollowerRole(this));
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
                        warn("State machine thread interrupted. Exiting");
                        return;
                    }
                }
            }
            info("Exiting state machine application thread");
        };
    }

    public synchronized void start() throws IOException, InterruptedException{
        if(!this.isRunning) {
            this.startStateMachineThread();
            this.server.start();
            this.requestStateTransition(ServerRole.Follower);
            this.isRunning = true;
            this.startTimerThread();
        }
    }

    public synchronized void stop(long maxWaitInMs) throws InterruptedException {
        if(this.isRunning) {
            this.server.stop(maxWaitInMs, TimeUnit.MILLISECONDS);
            this.stopTimerThread();
            this.stopStateMachineThread();
            for (RaftRole role : this.serverRoleRaftRoleMap.values()) {
                role.stop();
            }
        }
    }

    private synchronized void startTimerThread(){
        if(this.timerThread != null && this.timerThread.isAlive()){
            return;
        }
        this.stopTimerThread.set(false);
        this.timerThread.start();
    }

    private synchronized void stopTimerThread() throws InterruptedException{
        this.stopTimerThread.set(false);
        if(this.timerThread != null && this.timerThread.isAlive()){
            this.stateMachineThread.join();
        }
    }

    private synchronized void startStateMachineThread() throws InterruptedException{
        if(this.stateMachineThread.isAlive()) {
            stopStateMachineThread();
        }
        this.stopStateMachineApplicationThread.set(false);
        this.stateMachineThread.start();
    }

    private synchronized void stopStateMachineThread() throws InterruptedException{
        if(this.stateMachineThread.isAlive()){
            this.stopStateMachineApplicationThread.set(true);
            this.stateMachineThread.join();
        }
    }

    private synchronized void requestStateTransition(@NonNull ServerRole targetState){
        log.info("Stopping current role {}", serverRole);
        RaftRole raftRole = this.serverRoleRaftRoleMap.get(this.serverRole);
        raftRole.stop();
        ServerRole oldRole = this.serverRole;
        this.serverRole = targetState;
        log.info("Starting new role {}", serverRole);
        if(targetState == ServerRole.Leader) {
            this.tryUpdateLeader(this.serverId, this.serverState.getCurrentTerm());
        }
        this.serverRoleRaftRoleMap.get(targetState).start();
        this.fireServerRoleChangedEvent(oldRole, this.serverRole, this.getServerState().getCurrentTerm());
    }

    public synchronized void proposeConfigurationChange(RaftConfiguration raftConfiguration){
        throwIfNotRunning();
        throwInvalidRoleForOperation(ServerRole.Leader);

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
        throwIfNotRunning();
        throwInvalidRoleForOperation(ServerRole.Leader);

        info("proposed {} entries for term = {}",proposals.size(),
                this.serverState.getCurrentTerm());
        this.getWriteAheadLog().appendLogEntries(proposals.stream().map(p ->
        LogEntry.builder()
                .type(LogEntryType.NORMAL)
                .term(this.serverState.getCurrentTerm()).array(p).build())
                .collect(Collectors.toList()));
    }

    public void subscribe(@NonNull RaftServerEventListener listener) {
        this.eventListener.add(listener);
    }

    public void unsubscribe(@NonNull RaftServerEventListener listener) {
        this.eventListener.remove(listener);
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
        this.scheduledExecutorService.schedule(()-> this.requestStateTransition(ServerRole.Leader),0, TimeUnit.MILLISECONDS);
    }

    synchronized void recordMajorityHeartbeat(){
        this.lastAppendEntriesOrVoteGrantedOrMajorityResponse = Instant.now();
    }

    public synchronized ServerRole getServerRole() {
        return serverRole;
    }

    private void timerThreadLogic() {
        info("Starting timer logic");
        while(!this.stopTimerThread.get()){
            try {
                this.timeoutLogic();
            }catch (Exception e){
                error("Exception in timer thread logic ", e);
            }
            Uninterruptibles.sleepUninterruptibly(this.getRaftConfiguration().getRandomizedElectionTimeout(), TimeUnit.MILLISECONDS);
        }
        info("Exiting timer logic");
    }

    private synchronized void timeoutLogic(){
        info("Election timer fired {}",serverRole.toString());
        try {
            if (this.serverRole == ServerRole.Follower) {
            /*
             Rules for followers:
                If election timeout elapses without receiving AppendEntries
                RPC from current leader or granting vote to candidate:
                convert to candidate.
            */
                Duration elaspedRpcTime = Duration.between(lastAppendEntriesOrVoteGrantedOrMajorityResponse, Instant.now());
                if (//Check if we revd append entries
                        (elaspedRpcTime.compareTo(Duration.ofMillis(this.getRaftConfiguration().getMinElectionTimeoutInMs())) <= 0)) {
                    return;
                }
                info("Election timer expired converting to candidate elapsed time {}", elaspedRpcTime);
                this.requestStateTransition(ServerRole.Candidate);
            } else if (this.serverRole == ServerRole.Candidate) {
                //If we are candidate. Lets restart the election again
                this.requestStateTransition(ServerRole.Candidate);
            } else if (this.serverRole == ServerRole.Leader) {
                Duration lastMajorityHeartbeat = Duration.between(lastAppendEntriesOrVoteGrantedOrMajorityResponse, Instant.now());
                if (//Check if we revd majority heartbeat
                        (lastMajorityHeartbeat.compareTo(Duration.ofMillis(this.getRaftConfiguration()
                                .getMinElectionTimeoutInMs())) <= 0)) {
                    info("Still maintaining leadership as last majority heartbeat was at {}",
                            lastMajorityHeartbeat);
                    return;
                }
                info("Stepping down from being a leader as we could not hear from a majority");
                this.requestStateTransition(ServerRole.Candidate);
            }
        }catch (Exception e) {
            info("{} Exception in timeout handler ", e);
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
            info("Converting to follower due to term mismatch old term {} new term {}", oldTerm, term);
            this.requestStateTransition(ServerRole.Follower);
            return true;
        }else{
            return false;
        }
    }

    private synchronized boolean updateTerm(long term){
        if(this.serverState.getCurrentTerm() < term) {
            info("Term {} is > our term {}",term, this.serverState.getCurrentTerm());
            this.serverState.setVotedForAndCurrentTerm(-1,term);
            return true;
        }else{
            return false;
        }
    }

    @Override
    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        try {
            this.lastAppendEntriesOrVoteGrantedOrMajorityResponse = Instant.now();

            if (this.updateTermIfWeAreBehindAndConvertToFollower(request.getTerm())) {
                info("Append entries term is > our term ");
            }

            this.tryUpdateLeader(request.getLeaderId(), request.getTerm());

            debug("Current log status lastIndex {} lastTerm {} request prev Index {} prev Term {} entry count {}",
                    this.getWriteAheadLog().getLastEntryIndex(), this.getWriteAheadLog()
                            .getLogEntry(this.getWriteAheadLog().getLastEntryIndex()).getTerm(),
                    request.getPrevLogIndex(), request.getPrevLogTerm(), request.getEntries().length);
            AppendEntriesResponse.AppendEntriesResponseBuilder responseBuilder = AppendEntriesResponse.builder();
            responseBuilder.term(this.serverState.getCurrentTerm());
            if (request.getTerm() < this.serverState.getCurrentTerm()) {
                responseBuilder.succeeded(false);
                responseBuilder.lastLogIndexOnServer(this.getWriteAheadLog().getLastEntryIndex());
                return responseBuilder.build();
            }

            boolean logContainsEntryAtPrevIndex = this.getWriteAheadLog().getLastEntryIndex() >= request.getPrevLogIndex();
            if (!logContainsEntryAtPrevIndex) {
                responseBuilder.succeeded(false);
                responseBuilder.lastLogIndexOnServer(this.getWriteAheadLog().getLastEntryIndex());
                return responseBuilder.build();
            }

            boolean termsMatchForLogEntryAtPrevIndex = this.getWriteAheadLog().getLogEntry(request.getPrevLogIndex()).getTerm()
                    == request.getPrevLogTerm();
            if (!termsMatchForLogEntryAtPrevIndex) {
                responseBuilder.term(this.serverState.getCurrentTerm());
                responseBuilder.succeeded(false);
                long conflictingTerm = this.getWriteAheadLog().getLogEntry(request.getPrevLogIndex()).getTerm();
                long firstEntryForConflictingTerm = getFirstIndexOfTermInLog(conflictingTerm);
                responseBuilder.conflictDetails(AppendEntriesResponse.ConflictDetails.builder()
                        .conflictingTerm(conflictingTerm)
                        .logIndexOfFirstEntryInConflictingTerm(firstEntryForConflictingTerm)
                        .build());
                return responseBuilder.build();
            } else {
                responseBuilder.term(this.serverState.getCurrentTerm());
                responseBuilder.succeeded(true);
            }

        /*
         If an existing entry conflicts with a new one (same index
         but different terms), delete the existing entry and all that
         follow it (§5.3)
         Append any entries that raftlog doesn't contain
        */
            long nextLogIndex = request.getPrevLogIndex() + 1;
            int entriesToSkip = 0;

            //Check for conflict or look for entries that match to skip
            boolean didnotConflict = true;
            for (; entriesToSkip < request.getEntries().length && nextLogIndex <= this.getWriteAheadLog().getLastEntryIndex() &&
                    (didnotConflict = this.getWriteAheadLog().getLogEntry(nextLogIndex).getTerm() ==
                            request.getEntries()[entriesToSkip].getTerm());
                 entriesToSkip++, nextLogIndex++)
                ;
            //If a conflict is found remove all entries proceeding it including that entry
            if (nextLogIndex <= this.getWriteAheadLog().getLastEntryIndex() && !didnotConflict) {
                warn("Found conflicting entries at {}", nextLogIndex);
                this.getWriteAheadLog().removeEntriesStartingFromIndex(nextLogIndex);
            }

            //Append the remaining entries to the end of the raftlog if any
            if (entriesToSkip < request.getEntries().length) {
                if (entriesToSkip + request.getPrevLogIndex() != this.getWriteAheadLog().getLastEntryIndex()) {
                    error("CRAY CRAY Corruption");
                }
                info("Appending entries starting from {} upto including {}", entriesToSkip,
                        request.getEntries().length - 1);
                this.getWriteAheadLog().appendLogEntries(Arrays
                        .asList(request.getEntries()).subList(entriesToSkip, request.getEntries().length));
            }

            setLastCommitIndex(request.getLeaderCommit());

            responseBuilder.lastLogIndexOnServer(this.getWriteAheadLog().getLastEntryIndex());
            return responseBuilder.build();
        }catch (Exception e){
            log.error("Error in append entries ", e);
            throw e;
        }
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
            info("Recvd vote request. Converting to follower");
        }

        VoteResponse.VoteResponseBuilder responseBuilder = VoteResponse.builder();
        responseBuilder.term(this.serverState.getCurrentTerm());
        if(request.getTerm() < this.serverState.getCurrentTerm()){
            info("Vote request for {}: request term {} < current term {}. Not granting vote",
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

        boolean canGrantVoteToCandidate = this.serverState.getVotedFor() == -1 ||
                this.serverState.getVotedFor() == request.getCandidateId();

        boolean ourLogIsAtleastAsUpdateAsCandidate =(request.getLastLogTerm()>logTerm ||
                (request.getLastLogTerm() == logTerm && logIndex<= request.getLastLogIndex()));

        if(canGrantVoteToCandidate && ourLogIsAtleastAsUpdateAsCandidate) {
            info("Granting vote for {}", request.getCandidateId());
            this.serverState.setVotedFor(serverId);
            this.lastAppendEntriesOrVoteGrantedOrMajorityResponse = Instant.now();
            responseBuilder.voteGranted(true);
        }else{
            info("Rejecting vote for {} CanVote: {} logIsGood: {}",request.getCandidateId(),
                    canGrantVoteToCandidate, ourLogIsAtleastAsUpdateAsCandidate);
            responseBuilder.voteGranted(false);
        }
        return responseBuilder.build();
    }

    @Override
    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        return null;
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

    long getLastCommitIndex(){
        return this.serverState.getLastCommitedIndex();
    }

    long getCurrentLeaderId() {
        return this.currentLeaderId;
    }

    RaftRpcClient getClient(long peerId){
        Preconditions.checkArgument(this.getRaftConfiguration().getPeers().containsKey(peerId));
        return this.rpcClients
                .computeIfAbsent(peerId,
                        id -> this.getRaftConfiguration().getPeerList().get((int)peerId)
                                .getClient(Math.min(10,this.getRaftConfiguration().getMinElectionTimeoutInMs()/2)));
    }

    @VisibleForTesting
    synchronized void tryUpdateLeader(long potentialLeader, long potentialLeaderTerm){
        if((this.currentLeaderId == - 1 && potentialLeaderTerm == currentLeaderTerm)
                || potentialLeaderTerm > this.currentLeaderTerm) {
            this.currentLeaderId = potentialLeader;
            this.currentLeaderTerm = potentialLeaderTerm;
            fireLeaderChangedEventAsync();
        }else if(potentialLeaderTerm == currentLeaderTerm && potentialLeader != currentLeaderId){
            log.error("Invalid leader update. Raft guarantees exactly one leader per term. current leader {} current term {}, proposed leader {} proposed term {}",
                    currentLeaderId, currentLeaderTerm, potentialLeader, potentialLeaderTerm);
        }
    }

    public String toString(){
        return "ID " + this.serverId;
    }

    private synchronized void throwIfNotRunning(){
        if(!this.isRunning) {
            throw new IllegalStateException("This instance is not running. Call start() first");
        }
    }

    private synchronized void throwInvalidRoleForOperation(ServerRole expectedRole){
        if(this.serverRole != expectedRole) {
            throw new IllegalStateException("Expected to be in state " + expectedRole + " + but was in" + serverRole);
        }
    }


    private void debug(String message, Object... args) {
        this.log.debug(getServerId() + " " + message, args);
    }

    private void info(String message, Object... args) {
        this.log.info(getServerId() + " " + message, args);
    }

    private void warn(String message, Object... args) {
        this.log.warn(getServerId() + " " + message, args);
    }

    private void error(String message, Object... args) {
        this.log.error(getServerId() + " " + message, args);
    }

    private void error(String message, Throwable error){
        this.log.error(getServerId() + " "  + message, error);
    }

    private void fireLeaderChangedEventAsync(){
        long currentLeaderId = this.currentLeaderId;
        long currentLeaderTerm = this.currentLeaderTerm;
        CompletableFuture.runAsync(()-> {
            for (RaftServerEventListener listener : this.eventListener) {
                try {
                    listener.onLeaderChange(currentLeaderId, currentLeaderTerm);
                }catch (Exception e){
                    error("error firing leader change event", e);
                }
            }
        });
    }

    private void fireServerRoleChangedEvent(ServerRole old, ServerRole current, long currentTerm){
        CompletableFuture.runAsync(()-> {
            for (RaftServerEventListener listener : this.eventListener) {
                try {
                    listener.onServerStateRole(old, current, currentTerm);
                }catch (Exception e){
                    error("error firing leader change event", e);
                }
            }
        });
    }
}

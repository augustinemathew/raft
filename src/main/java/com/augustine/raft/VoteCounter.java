package com.augustine.raft;

import com.augustine.raft.rpc.VoteResponse;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
final class VoteCounter {
    private final int requiredVotes;
    private int numCompleted;
    private int numGranted;
    private int numRefused;
    private final HashMap<RaftPeer, CompletableFuture<VoteResponse>> watchedFutures = new HashMap<>();
    private HashMap<RaftPeer, Optional<Boolean>> voteStatus = new HashMap<>();
    private final CountDownLatch electionEnded = new CountDownLatch(1);
    private final long currentTerm;
    private Optional<Long> expectedTermForCandidate = Optional.empty();

    public VoteCounter(long currentTerm, int requiredVotes) {
        this.requiredVotes = requiredVotes;
        this.currentTerm = currentTerm;
    }

    public synchronized Optional<Long> getExpectedTermForCandidate() {
        return this.expectedTermForCandidate;
    }

    public synchronized boolean wonElection() {
        return numGranted >= requiredVotes;
    }

    public synchronized String getElectionResults() {
        Preconditions.checkState(this.electionEnded.getCount() == 0, "Election must end for election results");
        String status = wonElection() ? "Won" : "Failed";
        String ayes = "Ayes (" + this.numGranted + ") " + this.voteStatus.entrySet().stream()
                .filter(e -> e.getValue().isPresent() && e.getValue().get())
                .map(e -> e.getKey().getId())
                .collect(Collectors.toList()).toString();
        String nays = "Nays (" + this.numRefused + ") " + this.voteStatus.entrySet().stream()
                .filter(e -> !e.getValue().isPresent() || !e.getValue().get())
                .map(e -> e.getKey().getId())
                .collect(Collectors.toList()).toString();
        return status + " " + ayes + " " + nays + " required votes = " + this.requiredVotes;
    }

    public boolean waitForElectionToComplete(@NonNull Duration timeToWait) throws InterruptedException {
        try {
            return this.electionEnded.await(timeToWait.toMillis(), TimeUnit.MILLISECONDS);
        }catch (InterruptedException ie){
            return false;
        }
    }

    public synchronized void registerForCompletion(@NonNull  RaftPeer peer,
                                                   @NonNull CompletableFuture<VoteResponse> voteResponse) {
        if(watchedFutures.containsKey(peer)) {
            return;
        }
        voteResponse.whenComplete((r, e )-> {
            synchronized (this){
                if(e != null) {
                    if(!(e instanceof CancellationException)) {
                        log.error("An exception occurred while waiting for response from peer {} {}", peer.getId(), e);
                    }else{
                        log.warn("Election was cancelled. Skipping vote counting");
                    }
                    voteStatus.put(peer, Optional.empty());
                    return;
                }

                if(electionEnded.getCount() == 0) {
                    return;
                }

                numCompleted++;
                voteStatus.put(peer, Optional.of(r.isVoteGranted()));
                if(r.isVoteGranted()) {
                    if(r.getTerm() != currentTerm) {
                        log.error("Implementation bug! candidate granted vote for the wrong term expected {} {}",
                                currentTerm,
                                r.getTerm());
                        return;
                    }
                    numGranted++;
                }else {
                    numRefused++;
                }
                if(!r.isVoteGranted()) {
                    if(r.getTerm() > currentTerm) {
                        expectedTermForCandidate = Optional.of(r.getTerm());
                        electionEnded.countDown();
                    }
                }
                if(numGranted >= requiredVotes || numRefused>= requiredVotes) {
                    electionEnded.countDown();
                }
            }
        });
    }
}

package com.augustine.raft;

import com.augustine.raft.rpc.VoteRequest;
import com.augustine.raft.rpc.VoteResponse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.*;

public class RaftCandidateRole extends RaftRole {


    private volatile ScheduledFuture electionRound = null;

    public RaftCandidateRole(RaftServer server) {
        super(server);
    }

    public synchronized void start() {
        info("Attempting to start an election round");
        if(this.server.getServerRole() != ServerRole.Candidate) {
            error("Server is not candidate " + this.server.getServerRole());
            throw new RuntimeException("Not candidate invalid state");
        }

        this.stop();
        long newTerm = this.getRaftState().getCurrentTerm() + 1;
        this.getRaftState().setVotedForAndCurrentTerm(this.getServerId(), newTerm);
        this.server.tryUpdateLeader(-1, newTerm);
        this.electionRound = this.server.getScheduledExecutorService().schedule(this::electionLogic,0, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if(this.electionRound != null){
            info("Cancelling ongoing election ");
            try {
                this.electionRound.cancel(true);
                this.electionRound.get();
                info("Cancelled ongoing election success!");
            }catch (InterruptedException|CancellationException ie) {
                info("Cancelled or interrupted election");
            }catch (ExecutionException ee){
                warn("Previous execution returned with error",ee);
            }catch (Exception ee){
                error("Error in cancelling election ",ee);
                throw new RuntimeException(ee);
            }
            this.electionRound = null;
        }
    }

    private void electionLogic(){
        ArrayList<CompletableFuture<VoteResponse>> responses = new ArrayList<>();
        try {
            VoteRequest request = getVoteRequest();
            VoteCounter voteCounter = new VoteCounter(request.getTerm(), this.server.getMajority());
            for (RaftPeer peer : this.getRaftConfiguration().getPeerList()) {
                final String peerId = peer.toString();
                if(peer.getId() == this.getServerId()) {
                    voteCounter.registerForCompletion(peer, CompletableFuture.completedFuture(VoteResponse.builder()
                            .term(request.getTerm())
                            .voteGranted(true)
                            .build()));
                    continue;
                }else {

                    CompletableFuture<VoteResponse> response = server.getClient(peer.getId())
                            .RequestToVote(request)
                            .exceptionally((exception) -> {
                                error("Exception while talking to peer {}", exception, peerId);
                                return null;
                            });
                    responses.add(response);
                    voteCounter.registerForCompletion(peer, response);
                }
             }

            Duration timeToWait = Duration.ofMillis(this.getRaftConfiguration().getMinElectionTimeoutInMs());
            info("Waiting for vote responses and election to terminate with timeout {}", timeToWait);
            if(voteCounter.waitForElectionToComplete(timeToWait)) {
                info("Election results " + voteCounter.getElectionResults());
                if (voteCounter.wonElection()) {
                    this.server.tryConvertToLeader();
                } else if (voteCounter.getExpectedTermForCandidate().isPresent()) {
                    this.server.updateTermIfWeAreBehindAndTryConvertToFollower(voteCounter
                            .getExpectedTermForCandidate().get());
                }
            }else {
                warn("Election did not terminate in the amount of time required");
            }
        } catch (InterruptedException e) {
            warn("Interrupted in election");
        } catch (Exception e) {
            error("Unhandled exception in performing election. Exiting election logic", e);
        } finally {
            CompletableFutures.cancelAll(responses, true);
        }
    }

    private VoteRequest getVoteRequest(){
        VoteRequest.VoteRequestBuilder request = VoteRequest.builder();
        request.candidateId(this.getServerId());
        request.serverId(this.getServerId());
        //Copy last entry index as the proposals can update this value
        long lastEntryIndex = this.getWriteAheadLog().getLastEntryIndex();
        request.lastLogIndex(lastEntryIndex);
        request.term(this.getRaftState().getCurrentTerm());
        request.lastLogTerm(this.getWriteAheadLog().getLogEntry(lastEntryIndex).getTerm());
        return request.build();
    }
}


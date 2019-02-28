package com.augustine.raft;

import com.augustine.raft.rpc.VoteResponse;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class VoteCounterTests {
    static final RaftConfiguration raftConfiguration = RaftConfiguration.builder()
            .maxElectionTimeoutInMs(1000)
            .minElectionTimeoutInMs(500)
            .peerList(Arrays.asList(RaftPeer
                            .builder()
                            .dnsname("foobar")
                            .id(1)
                            .port(5000)
                            .build(),
                    RaftPeer
                            .builder()
                            .dnsname("foo")
                            .id(11)
                            .port(5001)
                            .build(),
                    RaftPeer
                            .builder()
                            .dnsname("foo2")
                            .id(12)
                            .port(5002)
                            .build()))
            .build();


    @Test
    public void testVoteCountingWin() throws InterruptedException{
        VoteCounter counter = new VoteCounter(10, 2);
        counter.registerForCompletion(raftConfiguration.getPeerList().get(0),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(true)
                        .term(10)
                        .build()));
        counter.registerForCompletion(raftConfiguration.getPeerList().get(1),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(true)
                        .term(10)
                        .build()));
        counter.waitForElectionToComplete(Duration.ZERO);
        Assert.assertTrue(counter.wonElection());
        Assert.assertFalse(counter.getExpectedTermForCandidate().isPresent());
    }

    @Test
    public void testVoteCountingLoseBecauseOfVoteFailure() throws InterruptedException{
        VoteCounter counter = new VoteCounter(10, 2);
        counter.registerForCompletion(raftConfiguration.getPeerList().get(0),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(false)
                        .term(10)
                        .build()));
        counter.registerForCompletion(raftConfiguration.getPeerList().get(1),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(false)
                        .term(10)
                        .build()));
        counter.waitForElectionToComplete(Duration.ZERO);
        Assert.assertFalse(counter.wonElection());
        Assert.assertFalse(counter.getExpectedTermForCandidate().isPresent());
    }

    @Test
    public void testVoteCountingLossBecausePeerAtHigherTerm() throws InterruptedException{
        VoteCounter counter = new VoteCounter(10, 2);
        counter.registerForCompletion(raftConfiguration.getPeerList().get(0),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(true)
                        .term(10)
                        .build()));
        counter.registerForCompletion(raftConfiguration.getPeerList().get(1),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(false)
                        .term(11)
                        .build()));
        counter.waitForElectionToComplete(Duration.ZERO);
        Assert.assertFalse(counter.wonElection());
        Assert.assertTrue(counter.getExpectedTermForCandidate().isPresent());
        Assert.assertEquals(Long.valueOf(11),counter.getExpectedTermForCandidate().get());
    }

    @Test
    public void testVoteCountingEarlyTerminationWin() throws InterruptedException{
        VoteCounter counter = new VoteCounter(10, 2);
        counter.registerForCompletion(raftConfiguration.getPeerList().get(0),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(true)
                        .term(10)
                        .build()));
        counter.registerForCompletion(raftConfiguration.getPeerList().get(1),
                executeWithDelay(VoteResponse.builder()
                        .voteGranted(true)
                        .term(10)
                        .build(), 10000));

        counter.registerForCompletion(raftConfiguration.getPeerList().get(1),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(true)
                        .term(10)
                        .build()));
        counter.waitForElectionToComplete(Duration.ZERO);
        Assert.assertTrue(counter.wonElection());
        Assert.assertFalse(counter.getExpectedTermForCandidate().isPresent());
    }

    @Test
    public void testVoteCountingEarlyTerminationLoss() throws InterruptedException{
        VoteCounter counter = new VoteCounter(10, 2);
        counter.registerForCompletion(raftConfiguration.getPeerList().get(0),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(false)
                        .term(10)
                        .build()));
        counter.registerForCompletion(raftConfiguration.getPeerList().get(1),
                executeWithDelay(VoteResponse.builder()
                        .voteGranted(true)
                        .term(10)
                        .build(), 10000));

        counter.registerForCompletion(raftConfiguration.getPeerList().get(1),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(false)
                        .term(10)
                        .build()));
        counter.waitForElectionToComplete(Duration.ZERO);
        Assert.assertFalse(counter.wonElection());
        Assert.assertFalse(counter.getExpectedTermForCandidate().isPresent());
    }

    @Test
    public void testVoteCountingGrantingVoteForWrongTerm() throws InterruptedException{
        VoteCounter counter = new VoteCounter(10, 2);
        counter.registerForCompletion(raftConfiguration.getPeerList().get(0),
                executeWithDelay(VoteResponse.builder()
                        .voteGranted(false)
                        .term(10)
                        .build(), 1000));
        counter.registerForCompletion(raftConfiguration.getPeerList().get(1),
                executeWithDelay(VoteResponse.builder()
                        .voteGranted(true)
                        .term(11)
                        .build(), 10000));

        counter.registerForCompletion(raftConfiguration.getPeerList().get(1),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(true)
                        .term(10)
                        .build()));
        Assert.assertFalse(counter.waitForElectionToComplete(Duration.ofMillis(2000)));
        Assert.assertFalse(counter.wonElection());
        Assert.assertFalse(counter.getExpectedTermForCandidate().isPresent());
    }

    @Test
    public void testVoteCountingBlocksForTermination() throws InterruptedException{
        VoteCounter counter = new VoteCounter(10, 2);
        counter.registerForCompletion(raftConfiguration.getPeerList().get(0),
                executeWithDelay(VoteResponse.builder()
                        .voteGranted(false)
                        .term(10)
                        .build(), 1000));
        counter.registerForCompletion(raftConfiguration.getPeerList().get(1),
                executeWithDelay(VoteResponse.builder()
                        .voteGranted(true)
                        .term(10)
                        .build(), 10000));

        counter.registerForCompletion(raftConfiguration.getPeerList().get(1),
                CompletableFuture.completedFuture(VoteResponse.builder()
                        .voteGranted(false)
                        .term(10)
                        .build()));
        counter.waitForElectionToComplete(Duration.ofMillis(2000));
        Assert.assertFalse(counter.wonElection());
        Assert.assertFalse(counter.getExpectedTermForCandidate().isPresent());
    }

    private static <T> CompletableFuture<T> executeWithDelay(T data, long delayInMs){
        return CompletableFuture.supplyAsync(()-> {
            Uninterruptibles.sleepUninterruptibly(delayInMs, TimeUnit.MILLISECONDS);
            return data;
        });
    }
}



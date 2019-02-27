package com.augustine.raft;

import lombok.NonNull;

import java.util.concurrent.atomic.AtomicBoolean;

public class RaftStateMachineApplicator {
    private final AtomicBoolean stopStateMachineApplicator = new AtomicBoolean(false);
    private final StateMachine stateMachine;
    private volatile Thread stateMachineThread;

    public RaftStateMachineApplicator(@NonNull StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }
}

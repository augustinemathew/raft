package com.augustine.raft;

import com.augustine.raft.snapshot.Snapshot;

public interface StateMachine{

    void apply(long lsn, byte[] input);

    void installSnapshot(Snapshot snapshot);

    Snapshot getSnapShot();
}

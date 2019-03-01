package com.augustine.raft;

import com.augustine.raft.snapshot.Snapshot;

public interface StateMachine{

    void apply(long lsn, byte[] input);

    void installSnapshot(Snapshot snapshot);

    Snapshot getSnapShot();

    static StateMachine NOOP_MACHINE(){
        return new StateMachine() {
            @Override
            public void apply(long lsn, byte[] input) {

            }

            @Override
            public void installSnapshot(Snapshot snapshot) {

            }

            @Override
            public Snapshot getSnapShot() {
                return null;
            }
        };
    }
}

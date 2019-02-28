package com.augustine.raft;

import lombok.NonNull;

public class RaftFollower extends RaftRole {
    protected RaftFollower(@NonNull RaftServer server) {
        super(server);
    }

    @Override
    protected void start() {

    }

    @Override
    protected void stop() {

    }
}

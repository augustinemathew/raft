package com.augustine.raft;

import lombok.NonNull;

public class RaftFollowerRole extends RaftRole {
    protected RaftFollowerRole(@NonNull RaftServer server) {
        super(server);
    }

    @Override
    protected void start() {

    }

    @Override
    protected void stop() {

    }
}

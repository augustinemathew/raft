package com.augustine.raft;

import com.augustine.raft.wal.Log;
import lombok.NonNull;

public abstract class RaftRole {
    protected final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(this.getClass());

    protected final RaftServer server;

    protected RaftRole(@NonNull RaftServer server) {
        this.server = server;
    }

    protected RaftConfiguration getRaftConfiguration() {
        return this.getRaftState().getLastKnownGoodConfiguration();
    }


    protected RaftState getRaftState() {
        return this.server.getServerState();
    }

    protected Log getWriteAheadLog() {
        return this.server.getServerConfig().getWriteAheadLog();
    }

    protected int getServerId() {
        return (int)this.server.getServerId();
    }

    protected void info(String message, Object... args) {
        this.log.info(getServerId() + " " + message, args);
    }

    protected void warn(String message, Object... args) {
        this.log.warn(getServerId() + " " + message, args);
    }

    protected void error(String message, Object... args) {
        this.log.error(getServerId() + " " + message, args);
    }

    protected void error(String message, Throwable error){
        this.log.error(getServerId() + " "  + message, error);
    }
}


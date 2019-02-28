package com.augustine.raft.wal;

public enum LogEntryType{
    NORMAL,
    CONFIG,
    SNAPSHOT,
    NO_OP,
}

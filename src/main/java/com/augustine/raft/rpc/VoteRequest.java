package com.augustine.raft.rpc;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder
public class VoteRequest extends RequestBase{
    private final long term;
    private final long candidateId;
    private final long lastLogIndex;
    private final long lastLogTerm;
    private final long serverId;
}

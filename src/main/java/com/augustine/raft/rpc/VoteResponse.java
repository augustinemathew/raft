package com.augustine.raft.rpc;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Builder
public class VoteResponse extends ResponseBase{
    private final long term;
    private final boolean voteGranted;
}

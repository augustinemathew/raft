package com.augustine.raft.rpc;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class InstallSnapshotResponse extends ResponseBase{
    private final long term;
    private final boolean ok;
}

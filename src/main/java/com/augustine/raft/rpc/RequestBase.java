package com.augustine.raft.rpc;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;


public abstract class RequestBase {

    public abstract long getServerId();
}

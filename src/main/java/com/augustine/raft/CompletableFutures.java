package com.augustine.raft;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CompletableFutures {

    public static <T> void cancelAll(@NonNull Collection<CompletableFuture<T>> futures, boolean mayInterruptIfRunning) {
        for(CompletableFuture future : futures) {
            future.cancel(mayInterruptIfRunning);
        }
    }
}

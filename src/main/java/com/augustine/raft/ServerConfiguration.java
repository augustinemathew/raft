package com.augustine.raft;

import com.augustine.raft.wal.Log;
import com.augustine.raft.wal.LogEntry;
import com.augustine.raft.wal.PersistentLog;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

@NoArgsConstructor
@AllArgsConstructor
public class ServerConfiguration {

    @Getter
    private long serverId;

    @Getter
    private String stateDir;

    @JsonIgnore
    private final  com.google.common.base.Supplier<RaftState>
            pstateSupplier = Suppliers
            .memoize(this::readRaftStateFromDisk);

    private final com.google.common.base.Supplier<Log> walSupplier = Suppliers.memoize(this::readWal);

    private RaftState readRaftStateFromDisk(){
        return new RaftState(getWalDirectoryPath(stateDir).toString());
    }

    private Log readWal(){
        return new PersistentLog(getWalDirectoryPath(stateDir).toString());
    }

    @JsonIgnore
    public RaftState getRaftState(){
        return this.pstateSupplier.get();
    }

    @JsonIgnore
    public Log getWriteAheadLog() {return  this.walSupplier.get();}

    public static void initializeServer(@NonNull String stateDir,
                                        int serverId,
                                        @NonNull RaftConfiguration lastGoodConfiguration){
        if(!Files.exists(Paths.get(stateDir))){
            new File(stateDir).mkdirs();
        }
        Path serverConfigPath = getServerConfigPath(stateDir);

        Preconditions.checkArgument(!Files.exists(serverConfigPath),
                "The directory must not be in use for another raft instance");
        Preconditions.checkArgument(!Files.exists(getWalDirectoryPath(stateDir)),
                "The directory must not be in use for another raft instance");
        Preconditions.checkArgument(!Files.exists(getRaftStateDirectoryPath(stateDir)),
                "The directory must not be in use for another raft instance");

        ServerConfiguration serverConfig = new ServerConfiguration(serverId, stateDir);
        try {
            ObjectMapper mapper = new ObjectMapper();
            Files.write(serverConfigPath, mapper.writeValueAsBytes(serverConfig));
        }catch (IOException e){
            throw new RuntimeException(e);
        }

        serverConfig.getRaftState()
                .setLastKnownGoodConfiguration(1,lastGoodConfiguration)
                .setVotedFor(-1)
                .setCurrentTerm(0)
                .setLastAppliedIndex(0);
        Preconditions.checkState(serverConfig.getWriteAheadLog().isEmpty());
        serverConfig.getWriteAheadLog().appendLogEntries(Arrays.asList(LogEntry.builder()
                .term(0)
                .configuration(lastGoodConfiguration)
                .build()));
    }


    public static ServerConfiguration fromDirectory(String stateDir){
        try {
            ObjectMapper mapper = new ObjectMapper();
            Path stateFilePath = getServerConfigPath(stateDir);
            return mapper.readValue(Files.readAllBytes(stateFilePath), ServerConfiguration.class);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    private static Path getServerConfigPath(@NonNull String stateDir) {
        return Paths.get(stateDir,"server.json");
    }

    private static Path getWalDirectoryPath(String stateDir) {
        return Paths.get(stateDir, "wal");
    }

    private static Path getRaftStateDirectoryPath(String stateDir){
        return Paths.get(stateDir,"raftState");
    }
}

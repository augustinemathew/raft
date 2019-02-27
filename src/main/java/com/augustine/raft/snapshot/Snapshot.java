package com.augustine.raft.snapshot;

import com.augustine.raft.RaftConfiguration;
import com.augustine.raft.proto.ProtoSerializerImpl;
import com.augustine.raft.proto.RaftRpc;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.common.io.PatternFilenameFilter;
import com.google.protobuf.ByteString;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Getter
public final class Snapshot{
    private final FileChannel snapshotFile;

    private static final int HEADER_LENGTH = 8;
    private static final int HEADER_BLOCK_LENGTH = 512;
    private final RaftRpc.SnapshotHeader.Builder header;
    private final Path snapshotFilePath;

    public Snapshot(long lsn,
                    long term,
                    @NonNull File snapshotDirectory) throws IOException {
        Preconditions.checkArgument(snapshotDirectory.isDirectory(),
                                    "Snapshot directory " + snapshotDirectory.getAbsolutePath());

        this.header = RaftRpc.SnapshotHeader.newBuilder();
        this.header.setLastIncludedIndex(lsn);
        this.header.setLastIncludedTerm(term);
        this.header.setIsComplete(false);
        this.snapshotFilePath = Paths.get(snapshotDirectory.getAbsolutePath(), UUID.randomUUID().toString());
        this.snapshotFile = FileChannel.open(snapshotFilePath,
                                             StandardOpenOption.CREATE_NEW,
                                             StandardOpenOption.READ,
                                             StandardOpenOption.WRITE);
    }

    public synchronized RaftConfiguration getConfiguration(){
        return new ProtoSerializerImpl().fromProtobuf(this.header.getLastConfiguration());
    }

    public synchronized long getLsn(){
        return this.header.getLastIncludedIndex();
    }

    public synchronized long getTerm(){
        return this.header.getLastIncludedTerm();
    }

    Snapshot(@NonNull File snapshotFilePath) throws IOException{
        this.snapshotFilePath = Paths.get(snapshotFilePath.getAbsolutePath());
        this.snapshotFile = FileChannel.open(this.snapshotFilePath,
                                             StandardOpenOption.READ);
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Long.BYTES);
        if(this.snapshotFile.read(lengthBuffer) != 8){
            throw new IllegalStateException("Invalid header length for file. Expected " + Long.BYTES);
        }
        int headerLength = (int)lengthBuffer.getLong();
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerLength);

        if(snapshotFile.read(headerBuffer) != headerLength) {
            throw new IllegalArgumentException("Invalid header. Expected to read " + headerLength + " bytes from file");
        }
        this.header = RaftRpc.SnapshotHeader.parseFrom(headerBuffer.array()).toBuilder();
        //Validate sha256
        Preconditions.checkState(this.header.getIsComplete(),"header must be finalized");
    }

    public synchronized void finalizeSnapshot(@NonNull RaftConfiguration lastGoodConfig) throws IOException {
        Preconditions.checkArgument(!this.header.getIsComplete(), "Completed snapshots are immutable");

        RaftRpc.RaftConfiguration serializedConfig = new ProtoSerializerImpl().toProtobuf(lastGoodConfig);
        this.header.setIsComplete(true);
        this.header.setLastConfiguration(serializedConfig);
        this.header.setSha256(ByteString.copyFrom(getSha256()));

        byte[] headerBytes = this.header.build().toByteArray();
        this.snapshotFile.position(0);
        this.snapshotFile.write(getHeaderLengthBytes(headerBytes.length));
        this.snapshotFile.write(ByteBuffer.wrap(headerBytes));
        this.snapshotFile.force(true);
        this.snapshotFile.close();
        Files.move(this.snapshotFilePath,
                   Paths.get(this.snapshotFilePath.getParent().toString(),
                             this.header.getLastIncludedIndex() + ".snap"),
                   StandardCopyOption.ATOMIC_MOVE,
                   StandardCopyOption.REPLACE_EXISTING);
    }

    private ByteBuffer getHeaderLengthBytes(long length) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(length);
        buffer.flip();
        return buffer;
    }

    private byte[] getSha256() throws IOException {

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            this.snapshotFile.position(HEADER_BLOCK_LENGTH + HEADER_LENGTH);
            ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
            int nread = -1;
            while ((nread = snapshotFile.read(buffer)) != 0 && nread != -1) {
                buffer.flip();
                digest.update(buffer);
            }
            byte[] computedSha256 = digest.digest();
            return computedSha256;
        }catch (IOException ioe){
            throw ioe;
        }catch (NoSuchAlgorithmException noe){
            throw new RuntimeException(noe);
        }
    }
}


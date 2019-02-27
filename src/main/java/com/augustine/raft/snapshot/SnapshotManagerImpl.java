//package com.augustine.raft.snapshot;
//
//import com.augustine.raft.RaftConfiguration;
//import com.augustine.raft.proto.ProtoSerializer;
//import com.augustine.raft.proto.ProtoSerializerImpl;
//import com.augustine.raft.proto.RaftRpc;
//import com.google.common.primitives.Longs;
//import com.google.protobuf.InvalidProtocolBufferException;
//import lombok.Builder;
//import lombok.Getter;
//import lombok.NonNull;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.channels.FileChannel;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.nio.file.StandardCopyOption;
//import java.nio.file.StandardOpenOption;
//import java.security.MessageDigest;
//import java.security.NoSuchAlgorithmException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.stream.Collectors;
//
// final class SnapshotFile{
//    private static final int HEADER_LENGTH = 8;
//    private static final int HEADER_BLOCK_LENGTH = 512;
//
//    @Getter
//    private SnapshotManagerImpl.SnapshotHeader header;
//    private final File snapshotDirectory;
//    private File snapshotMetadataFile;
//    private File snapshotFile;
//
//    @Getter
//    private File file;
//
//     public static File getSnapshotMetadataFile(File snapshotDirectory, long lsn, long term) {
//         return new File(Paths.get(snapshotDirectory.getAbsolutePath(),
//                 lsn +"." + term + ".meta").toString());
//     }
//
//     public static File getSnapshotFile(File snapshotDirectory,long lsn, long term) {
//         return new File(Paths.get(snapshotDirectory.getAbsolutePath(),
//                 lsn +"." + term + ".snap").toString());
//     }
//
//    public SnapshotFile(@NonNull File snapshotDirectory,
//                        long lsn, long term) throws IOException{
//        this.snapshotDirectory = snapshotDirectory;
//        this.snapshotMetadataFile = getSnapshotMetadataFile(snapshotDirectory,lsn,term);
//        this.snapshotFile = getSnapshotFile(snapshotDirectory,lsn,term);
//
//        if(!this.snapshotMetadataFile.exists()){
//            this.header = SnapshotManagerImpl.SnapshotHeader.builder()
//                    .lastIncludedIndex(lsn)
//                    .lastIncludedTerm(term)
//                    .lastConfiguration(RaftConfiguration.builder()
//                            .peerList(new ArrayList<>())
//                            .build())
//                    .sha256(new byte[0])
//                    .build();
//            this.saveMetadata();
//        }else{
//            this.header = readHeader();
//        }
//    }
//
//
//    public void append(long offset, @NonNull byte[] data) throws IOException{
//         if(this.snapshotFile == null){
//             throw new RuntimeException("Snapshot deleted");
//         }
//        FileChannel channel = FileChannel.open(this.snapshotFile.toPath(),
//                StandardOpenOption.CREATE,
//                StandardOpenOption.WRITE);
//
//        channel.position(offset);
//        try {
//            channel.write(ByteBuffer.wrap(data), offset);
//            channel.close();
//        }catch (IOException e){
//            channel.close();
//            throw e;
//        }
//    }
//
//    public void saveSnapshot(@NonNull byte[] sha256,
//                             @NonNull RaftConfiguration configuration)
//            throws IOException{
//        if(this.snapshotFile == null){
//            throw new RuntimeException("Snapshot deleted");
//        }
//        try {
//            this.header = this.header.toBuilder()
//                    .lastConfiguration(configuration)
//                    .sha256(sha256)
//                    .build();
//            byte[] computedSha256 = getSha256();
//            if(Arrays.equals(sha256, computedSha256)){
//                this.saveMetadata();
//            }else{
//                deleteSnapshot();
//                throw new RuntimeException("Checksum mismatch");
//            }
//        }catch (NoSuchAlgorithmException ne){
//            throw new RuntimeException(ne);
//        }
//    }
//
//     public void deleteSnapshot() throws IOException {
//         if(this.snapshotFile == null) {
//            return;
//         }
//
//         Files.delete(this.snapshotFile.toPath());
//         this.snapshotFile = null;
//         Files.delete(this.snapshotMetadataFile.toPath());
//         this.snapshotMetadataFile = null;
//     }
//
//     public byte[] getSha256() throws IOException, NoSuchAlgorithmException {
//        MessageDigest digest = MessageDigest.getInstance("SHA-256");
//        FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE);
//        try {
//
//            channel.position(HEADER_BLOCK_LENGTH + HEADER_LENGTH);
//            ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
//            int nread = -1;
//            while ((nread = channel.read(buffer)) != 0 && nread != -1) {
//                buffer.flip();
//                digest.update(buffer);
//            }
//            byte[] computedSha256 = digest.digest();
//            channel.close();
//            return computedSha256;
//        }catch (IOException ioe){
//            channel.close();
//            throw ioe;
//        }
//    }
//
//     private SnapshotManagerImpl.SnapshotHeader readHeader() throws IOException{
//         FileChannel channel = FileChannel.open(file.toPath(),
//                 StandardOpenOption.READ);
//         try {
//             ByteBuffer headerBlockLengthBytes = ByteBuffer.allocate(HEADER_LENGTH);
//             channel.read(headerBlockLengthBytes);
//             headerBlockLengthBytes.flip();
//             long headerBlockLength = headerBlockLengthBytes.getLong();
//             ByteBuffer headerBytes = ByteBuffer.allocate((int) headerBlockLength);
//             channel.read(headerBytes);
//             headerBytes.flip();
//             return SnapshotManagerImpl.SnapshotHeader.fromBytes(headerBytes);
//         }catch (IOException ioe){
//             channel.close();
//             throw ioe;
//         }
//     }
//
//     private void saveMetadata() throws IOException{
//         final Path tempPath = File.createTempFile("snapshotmeta", "snapdir").toPath();
//         FileChannel channel = FileChannel.open(
//                 tempPath,
//                 StandardOpenOption.WRITE,
//                 StandardOpenOption.CREATE);
//         try {
//             ByteBuffer headerBlockLengthBytes = ByteBuffer.allocate(HEADER_LENGTH);
//             byte[] headerBytes = this.header.toBytes();
//             headerBlockLengthBytes.putLong(headerBytes.length);
//             headerBlockLengthBytes.flip();
//             channel.write(headerBlockLengthBytes);
//             System.out.println(channel.position());
//             ByteBuffer headerBlock = ByteBuffer.allocate(HEADER_BLOCK_LENGTH);
//             headerBlock.put(headerBytes);
//             headerBlock.flip();
//             channel.write(headerBlock);
//             System.out.println(channel.position());
//             channel.close();
//             Files.move(tempPath,this.snapshotMetadataFile.toPath(),
//                     StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
//         }catch (IOException ioe){
//             channel.close();
//             throw ioe;
//         }
//     }
//
// }
//
//
//public final class SnapshotManagerImpl implements SnapshotManager {
//    private final File snapshotDirectory;
//    private static final ProtoSerializer serializer = new ProtoSerializerImpl();
//
//    @Builder(toBuilder = true)
//    @Getter
//    public static final class SnapshotHeader{
//        private final long lastIncludedIndex;
//        private final long lastIncludedTerm;
//        private RaftConfiguration lastConfiguration;
//        private boolean isComplete;
//        private byte[] sha256;
//
//        public static SnapshotHeader fromBytes(@NonNull ByteBuffer bytes){
//            try {
//                return serializer.fromProtobuf(RaftRpc.SnapshotHeader.parseFrom(bytes.array()));
//            }catch (InvalidProtocolBufferException ipe){
//                throw new RuntimeException(ipe);
//            }
//        }
//
//        public byte[] toBytes(){
//            return serializer.toProtobuf(this).toByteArray();
//        }
//    }
//
//
//    public SnapshotManagerImpl(@NonNull File snapshotDirectory) {
//        if(!snapshotDirectory.exists()){
//            snapshotDirectory.mkdirs();
//        }
//        this.snapshotDirectory = snapshotDirectory;
//    }
//
//    public List<Snapshot> listSnapshots() throws IOException {
//        List<Snapshot> snapshots = Arrays
//                .asList(snapshotDirectory
//                .list((dir,name)->name.endsWith(".snap")))
//                .stream()
//                .map(f -> {
//                    String[] fileParts = f.split("\\.");
//
//                    if(fileParts.length==3){
//                        Long index = Longs.tryParse(fileParts[0]);
//                        Long term =Longs.tryParse(fileParts[1]);
//
//                        if(index != null && term != null) {
//                            try {
//                                final String filePath = Paths.
//                                        get(snapshotDirectory.toString(), f).toString();
//                                SnapshotFile file = new SnapshotFile(this.snapshotDirectory, index, term);
//                                return new Snapshot(index, term,
//                                        file.getSha256(),
//                                        file.getHeader().lastConfiguration,
//                                        SnapshotFile.getSnapshotFile(this.snapshotDirectory,index, term));
//                            }catch (NoSuchAlgorithmException noe){
//                                throw new RuntimeException(noe);
//                            }
//                            catch (IOException ioe){
//                                throw new RuntimeException(ioe);
//                            }
//                        }
//                    }
//                    return null;
//                })
//                .filter(m -> m != null)
//                .collect(Collectors.toList());
//
//        return snapshots;
//    }
//
//    @Override
//    public void appendToSnapshot(long lsn, long term, long offset, @NonNull byte[] data) throws IOException {
//        SnapshotFile snapFile = new SnapshotFile(this.snapshotDirectory,lsn, term);
//        snapFile.append(offset,data);
//    }
//
//    public Snapshot saveSnapshot(long lsn, long term, RaftConfiguration configuration, @NonNull byte[] sha256) throws IOException{
//        SnapshotFile snapFile = new SnapshotFile(this.snapshotDirectory, lsn, term);
//        snapFile.saveSnapshot(sha256,configuration);
//        return new Snapshot(lsn, term,sha256,
//                snapFile.getHeader().lastConfiguration,snapFile.getFile());
//    }
//}

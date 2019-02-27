//package com.augustine.raft;
//
//import com.augustine.raft.snapshot.Snapshot;
//import com.augustine.raft.snapshot.SnapshotManagerImpl;
//import junit.framework.Assert;
//import junit.framework.TestCase;
//
//import java.io.ByteArrayOutputStream;
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.security.MessageDigest;
//import java.security.NoSuchAlgorithmException;
//import java.util.Arrays;
//import java.util.Optional;
//import java.util.Random;
//
//public class SnapshotManagerTests extends TestCase {
//
//    public SnapshotManagerTests(String testName ) {
//        super( testName );
//    }
//
//    public void testSnapshotManagerE2E() throws IOException, NoSuchAlgorithmException{
//        SnapshotManagerImpl imp = new SnapshotManagerImpl(new File("/tmp/snaps"));
//
//        Random r = new Random();
//        final int blockSize = 10;
//        byte[] bytes = new byte[blockSize];
//        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
//        MessageDigest digest = MessageDigest.getInstance("SHA-256");
//        for(int i = 0; i< blockSize; i++){
//            r.nextBytes(bytes);
//            imp.appendToSnapshot(10,1, blockSize *i, bytes);
//            outputStream.write(bytes);
//            digest.update(bytes);
//        }
//
//        final byte[] sha256 = digest.digest();
//        imp.saveSnapshot(10,1, RaftConfiguration.builder()
//                        .maxElectionTimeoutInMs(1000).build(),
//                sha256);
//        Optional<Snapshot> snap = imp.listSnapshots()
//                .stream()
//                .filter(f -> f.getLsn() == 10)
//                .findFirst();
//        Assert.assertTrue(snap.isPresent());
//        final Snapshot snapshot = snap.get();
//
//        Assert.assertTrue(snapshot.getLastConfiguration().getMaxElectionTimeoutInMs() == 1000);
//        Assert.assertTrue(snapshot.getSnapshotFile().exists());
//        byte[] readByteArray = Files.readAllBytes(snapshot.getSnapshotFile().toPath());
//
//        byte[] subArray = Arrays.copyOfRange(readByteArray,512+8,readByteArray.length);
//
//        Assert.assertTrue(Arrays.equals(outputStream.toByteArray(),subArray));
//        Assert.assertTrue(Arrays.equals(sha256, snapshot.getSha256()));
//        Assert.assertEquals(10,snapshot.getLsn());
//        Assert.assertEquals(1,snapshot.getTerm());
//    }
//}

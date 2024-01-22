package unipi.aide.mircv.model;


import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.MissingCollectionStatisticException;
import unipi.aide.mircv.log.CustomLogger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class CollectionStatistics {

    private static int collectionSize;      // size of the collection (number of documents)
    private static long documentsLen;       // sum of the length of the docs
    private static long numberOfTokens;      // size of the lexicon

    public static void updateDocumentsLen(int size) { documentsLen += size;}

    public static void updateCollectionSize() { collectionSize++; }

    public static int getCollectionSize(){return collectionSize;}

    public static void updateNumberOfToken(long value) {
        numberOfTokens = numberOfTokens + value;
    }

    public static long getNumberOfTokens() { return numberOfTokens; }

    public static double getAverageDocumentLength() { return documentsLen/(double)collectionSize;}

    public static void writeToDisk() {
        try {
            ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES * 2 + Integer.BYTES);
            FileChannel stream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getCollectionStatisticsPath()), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            buffer.putInt(collectionSize);
            buffer.putLong(documentsLen);
            buffer.putLong(numberOfTokens);
            buffer.flip();
            stream.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readFromDisk() throws MissingCollectionStatisticException {
        try(FileChannel stream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getCollectionStatisticsPath()), StandardOpenOption.READ)){
            ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES * 2 + Integer.BYTES);
            stream.read(buffer);
            buffer.flip();
            collectionSize = buffer.getInt();
            documentsLen = buffer.getLong();
            numberOfTokens = buffer.getLong();
        } catch (IOException e) {
            CustomLogger.error("Unable to read Collection Statics");
            throw new MissingCollectionStatisticException();
        }
    }

    public static void print() {
        System.out.println("CollectionStatistics:\n size: "+collectionSize+
                "\n documentsLen: "+documentsLen+
                "\n: numberOfTokens: "+numberOfTokens);
    }
}

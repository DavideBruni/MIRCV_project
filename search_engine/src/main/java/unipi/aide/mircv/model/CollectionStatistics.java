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

    private static int lengthLongestTerm;   // length of the longest term in the collection, usefull to fix a size for the LexiconEntry

    private static long numberOfTokens = 0;      // size of the lexicon

    public static void updateDocumentsLen(int size) { documentsLen += size;}

    public static void updateCollectionSize() { collectionSize++; }

    public static int getCollectionSize(){ return collectionSize;}

    public static void writeToDisk() {
        try {
            ByteBuffer buffer = ByteBuffer.allocateDirect(24);
            FileChannel stream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getCollectionStatisticsPath()), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            buffer.putInt(collectionSize);
            buffer.putLong(documentsLen);
            buffer.putInt(lengthLongestTerm);
            buffer.putLong(numberOfTokens);
            buffer.flip();
            stream.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readFromDisk() throws MissingCollectionStatisticException {
        try(FileChannel stream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getCollectionStatisticsPath()), StandardOpenOption.READ)){
            ByteBuffer buffer = ByteBuffer.allocateDirect(24);
            stream.read(buffer);
            buffer.flip();
            collectionSize = buffer.getInt();
            documentsLen = buffer.getLong();
            lengthLongestTerm = buffer.getInt();
            numberOfTokens = buffer.getLong();
        } catch (IOException e) {
            CustomLogger.error("Unable to read Collection Statics");
            throw new MissingCollectionStatisticException();
        }
    }


    public static long getDocumentsLen() { return documentsLen; }

    public static void setLongestTerm(int length) {
        if (length > lengthLongestTerm)
            lengthLongestTerm = length;
    }

    public static int getLongestTermLength() { return lengthLongestTerm; }

    public static void updateNumberOfToken(long value) {
        numberOfTokens = numberOfTokens + value;
    }

    public static long getNumberOfTokens() { return numberOfTokens; }

    // used for testing purpose only
    public static void reset(){
        collectionSize = 0;
        documentsLen = 0;
        lengthLongestTerm = 0;
        numberOfTokens = 0;
    }

    public static void print() {
        System.out.println("CollectionStatistics:\n size: "+collectionSize+
                "\n documentsLen: "+documentsLen+"\n lengthLongestterm: "+lengthLongestTerm +
                "\n: numberOfTokens: "+numberOfTokens);
    }
}

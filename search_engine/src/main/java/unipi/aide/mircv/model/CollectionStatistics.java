package unipi.aide.mircv.model;


import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.MissingCollectionStatisticException;
import unipi.aide.mircv.log.CustomLogger;

import java.io.*;

public class CollectionStatistics {

    private static int collectionSize;      // size of the collection (number of documents)

    private static long lexiconSize;        // size of the lexicon

    private static long documentsLen;       // sum of the length of the docs

    private static int lengthLongestTerm;   // length of the longest term in the collection, usefull to fix a size for the LexiconEntry

    private static int numberOfTokens;      // necessary to perform binary search on Lexicon

    public static void updateDocumentsLen(int size) { documentsLen += size;}

    public static void updateCollectionSize() { collectionSize++; }

    public static int getCollectionSize(){ return collectionSize;}

    public static void writeToDisk() {
        File file = new File(Configuration.getCollectionStatisticsPath());
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(file))) {
            dataOutputStream.writeInt(collectionSize);
            dataOutputStream.writeLong(lexiconSize);
            dataOutputStream.writeLong(documentsLen);
            dataOutputStream.writeInt(lengthLongestTerm);
            dataOutputStream.writeInt(numberOfTokens);
        } catch (
        IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readFromDisk() throws MissingCollectionStatisticException {
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(Configuration.getCollectionStatisticsPath()))) {
            collectionSize = dataInputStream.readInt();
            lexiconSize = dataInputStream.readLong();
            documentsLen = dataInputStream.readLong();
            lengthLongestTerm = dataInputStream.readInt();
            numberOfTokens = dataInputStream.readInt();
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

    public static void setNumberOfToken(int length) { numberOfTokens = length; }

    public static int getNumberOfTokens() { return numberOfTokens; }

    // used for testing purpose only
    static void reset(){
        collectionSize = 0;
        lexiconSize = 0;
        documentsLen = 0;
        lengthLongestTerm = 0;
        numberOfTokens = 0;
    }

}

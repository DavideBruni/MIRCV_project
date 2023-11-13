package unipi.aide.mircv.model;


import java.io.*;

public class CollectionStatistics {

    private static int collectionSize;     // size of the collection (number of documents)

    private static long lexiconSize;        // size of the lexicon

    private static long documentsLen;        //sum of the length of the docs

    private static final String COLLECTION_STATISTICS_PATH = "data/invertedIndex/collectionStatistics.dat";  //Path to the collection size

    public static void updateDocumentsLen(int size) {
        documentsLen += size;
    }

    public static void updateCollectionSize() {
        collectionSize++;
    }

    public static int getCollectionSize(){ return collectionSize;}

    public static void writeToDisk() {
        File file = new File(COLLECTION_STATISTICS_PATH);
        try (DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(file))) {
            dataOutputStream.writeInt(collectionSize);
            dataOutputStream.writeLong(lexiconSize);
            dataOutputStream.writeLong(documentsLen);
        } catch (
        IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void readFromDisk() {
        try (DataInputStream dataOutputStream = new DataInputStream(new FileInputStream(COLLECTION_STATISTICS_PATH))) {
            collectionSize = dataOutputStream.readInt();
            lexiconSize = dataOutputStream.readLong();
            documentsLen = dataOutputStream.readLong();
        } catch (IOException e) {
            // aggiungere log
        }
    }


    public static long getDocumentsLen() {
        return documentsLen;
    }
}

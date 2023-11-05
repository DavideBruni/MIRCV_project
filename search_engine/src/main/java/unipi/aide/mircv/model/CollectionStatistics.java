package unipi.aide.mircv.model;

public class CollectionStatistics {

    private static int collectionSize;     // size of the collection (number of documents)

    private static long lexiconSize;        // size of the lexicon

    private static long documentsLen;        //sum of the length of the docs

    private static final String COLLECTION_STATISTICS_PATH = "";  //Path to the collection size

    public static void updateDocumentsLen(int size) {
        documentsLen += size;
    }

    public static void updateCollectionSize() {
        collectionSize++;
    }

    public static int getCollectionSize(){ return collectionSize;}
}

package unipi.aide.mircv.model;

public class CollectionStatistics {

    private static long collectionSize;     // size of the collection

    private static long lexiconSize;        // size of the lexicon

    private static long documentsLen;        //sum of the length of the docs

    private static final String COLLECTION_STATISTICS_PATH = "";  //Path to the collection size

    public static void updateDocumentsLen(int size) {
        documentsLen += size;
    }
}

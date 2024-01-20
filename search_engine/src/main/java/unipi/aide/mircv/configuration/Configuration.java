package unipi.aide.mircv.configuration;

public class Configuration {
    private static String SCORE_STANDARD = "TFIDF";     // score standard, TFIDF is used as default
    private static boolean COMPRESSED;
    private static int MINHEAP_DIMENSION;

    /** ----------------------------------- PATHS ----------------------------------- **/
    private static String ROOT_DIRECTORY = "data";
    private static String DOCUMENT_IDS_PATH = ROOT_DIRECTORY+"/invertedIndex/document_ids.dat";
    private static String FREQUENCY_PATH = ROOT_DIRECTORY+"/invertedIndex/frequencies.dat";
    private static String LEXICON_PATH = ROOT_DIRECTORY+"/invertedIndex/lexicon.dat";
    private static String COLLECTION_STATISTICS_PATH = ROOT_DIRECTORY+"/invertedIndex/collectionStatistics.dat";
    private static String DOCUMENT_INDEX_PATH = ROOT_DIRECTORY+"/invertedIndex/";
    public static final int BLOCK_TRESHOLD = 2048;


    public static boolean isCOMPRESSED() {
        return COMPRESSED;
    }

    public static void setCOMPRESSED(boolean COMPRESSED) {
        Configuration.COMPRESSED = COMPRESSED;
        if(COMPRESSED){
            setUpPaths("data/compressed");
        }else{
            setUpPaths("data");
        }
    }

    public static void setMinheapDimension(int minheapDimension) {
        MINHEAP_DIMENSION = minheapDimension;
    }

    public static int getMinheapDimension() {
        return MINHEAP_DIMENSION;
    }

    public static void setUpPaths(String rootPaths) {
        ROOT_DIRECTORY = rootPaths;
        DOCUMENT_IDS_PATH = ROOT_DIRECTORY+"/invertedIndex/document_ids.dat";
        FREQUENCY_PATH = ROOT_DIRECTORY+"/invertedIndex/frequencies.dat";
        LEXICON_PATH = ROOT_DIRECTORY+"/invertedIndex/lexicon.dat";
        COLLECTION_STATISTICS_PATH = ROOT_DIRECTORY+"/invertedIndex/collectionStatistics.dat";
        DOCUMENT_INDEX_PATH = ROOT_DIRECTORY+"/invertedIndex/";
    }

    public static String getDocumentIdsPath() {
        return DOCUMENT_IDS_PATH;
    }

    public static String getFrequencyPath() {
        return FREQUENCY_PATH;
    }

    public static String getLexiconPath() {
        return LEXICON_PATH;
    }

    public static String getRootDirectory() {
        return ROOT_DIRECTORY;
    }

    public static String getCollectionStatisticsPath() {
        return COLLECTION_STATISTICS_PATH;
    }


    public static String getDocumentIndexPath() {
        return DOCUMENT_INDEX_PATH;
    }


    public static String getScoreStandard() {
        return SCORE_STANDARD;
    }

    public static void setScoreStandard(String standard) {
        SCORE_STANDARD = standard;
    }


}

package unipi.aide.mircv.configuration;

public class Configuration {
    private static boolean COMPRESSED;
    private static int MINHEAP_DIMENSION;

    public static final String DOCUMENT_IDS_PATH = "data/invertedIndex/document_ids.dat";
    public static final String FREQUENCY_PATH = "data/invertedIndex/frequencies.dat";


    public static boolean isCOMPRESSED() {
        return COMPRESSED;
    }

    public static void setCOMPRESSED(boolean COMPRESSED) {
        Configuration.COMPRESSED = COMPRESSED;
    }

    public static void setMinheapDimension(int minheapDimension) {
        MINHEAP_DIMENSION = minheapDimension;
    }

    public static int getMinheapDimension() {
        return MINHEAP_DIMENSION;
    }
}

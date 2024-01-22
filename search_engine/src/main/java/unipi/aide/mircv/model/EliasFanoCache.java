package unipi.aide.mircv.model;
/**
 * Utility class to store caching information for Elias-Fano compression.
 * This class is designed to store and retrieve caching information such as
 * high bits offset, the number of cached document IDs, and the current high bit number.
 * It is used to speed up write and read operations during Elias-Fano compression.
 */
public class EliasFanoCache {
    private long highBitsOffset;
    private int numberOfDocIds;
    private int currentHighBitNumber;

     EliasFanoCache() {
        highBitsOffset = -1;
        numberOfDocIds = -1;
        currentHighBitNumber = -1;
    }

    void setCache(long highBitsOffset, int numberOfDocIds, int currentHighBitNumber) {
        this.highBitsOffset = highBitsOffset;
        this.numberOfDocIds = numberOfDocIds;
        this.currentHighBitNumber = currentHighBitNumber;
    }

    long getHighBitsOffset() { return highBitsOffset; }

    int getNumberOfDocIdsCached() { return numberOfDocIds; }

    int getCurrentHighBitNumber() { return currentHighBitNumber; }
}

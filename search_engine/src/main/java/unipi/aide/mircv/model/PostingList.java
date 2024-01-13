package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;

import java.io.*;
import java.nio.channels.FileChannel;

public abstract class PostingList{

    protected static final String TEMP_DOC_ID_DIR ="/invertedIndex/temp/docIds";
    protected static final String TEMP_FREQ_DIR ="/invertedIndex/temp/frequencies";
    protected BlockDescriptor blockDescriptor;
    protected LexiconEntry lexiconEntry;
    protected int currentIndexPostings;


    public static UncompressedPostingList readFromDisk(int partition, int docIdOffset, int frequencyOffset, boolean compressed) throws FileNotFoundException{
        if (compressed)
                return CompressedPostingList.readFromDisk(partition, docIdOffset,frequencyOffset);
        return UncompressedPostingList.readFromDisk(partition, docIdOffset,frequencyOffset);
    }

    public abstract void add(int docId, int frequency);

    /**
     * Retrieves the current document ID from the PostingList.
     *
     * @return The current document ID from the PostingList.
     */
    public abstract int docId();

    public abstract double score();

    /**f
     * Advances to the next document in the PostingList, updating the internal state.
     */
    public abstract void next();

    public static PostingList loadFromDisk(String term) throws IOException {
        LexiconEntry lexiconEntry = Lexicon.getEntry(term, true);
        if (lexiconEntry != null) {
            if(Configuration.isCOMPRESSED())
                return CompressedPostingList.loadFromDisk(lexiconEntry);
            return UncompressedPostingList.loadFromDisk(lexiconEntry);
        }
        return null;
    }

    public abstract void nextGEQ(int docId);

     public double getTermUpperBound() {
        return lexiconEntry.getScoreTermUpperBound();
    }

    public abstract int[] writeOnDisk(FileChannel docStream, FileChannel freqStream, int[] offsets) throws IOException;
}

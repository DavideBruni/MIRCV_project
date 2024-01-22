package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.UnableToWriteLexiconException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class LexiconEntry {
    private int df;                         // Document Frequency
    private int docIdOffset;
    private int frequencyOffset;
    private int docIdLength;                // both length useful to load compressed posting list in memory during query processing
    private int frequencyLength;
    private int maxDocId;                    // used only during SPIMI and merge operations
    private double BM25_termUpperBound;      // both upper bounds used max score for dynamic pruning
    private double TFIDF_termUpperBound;

    /* ---------------------- CONSTRUCTORS -------------------- */
    public LexiconEntry(){
        df = 1;
    }

    public LexiconEntry(int df, int docIdOffset, int frequencyOffset, int docIdLength, int frequencyLength, double BM25_termUpperBound,double TFIDF_termUpperBound) {
        this.df = df;
        this.docIdOffset = docIdOffset;
        this.frequencyOffset = frequencyOffset;
        this.BM25_termUpperBound = BM25_termUpperBound;
        this.TFIDF_termUpperBound = TFIDF_termUpperBound;
        this.docIdLength = docIdLength;
        this.frequencyLength = frequencyLength;
    }
    /* ---------------------- END CONSTRUCTORS -------------------- */

    /* If not merged, dimension is lower, because I don't save a lot of information like upper bounds (see writeOnDisk)*/
    public static int getEntryDimension(boolean is_merged) {
        return is_merged ? 36 : 16;
    }

    public void writeOnDisk(String token) throws UnableToWriteLexiconException {
        File file = new File(Configuration.getLexiconPath());
        try {
            FileChannel stream = (FileChannel) Files.newByteChannel(file.toPath(),
                    StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE);
            writeOnDisk(stream, token, true);
        }catch (IOException e){
            throw new UnableToWriteLexiconException(e.getMessage());
        }
    }

    public void writeOnDisk(FileChannel stream, String key, boolean is_merged) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(Lexicon.TERM_DIMENSION+LexiconEntry.getEntryDimension(is_merged));
        buffer.put(stringToArrayByteFixedDim(key));
        buffer.putInt(df);
        buffer.putInt(docIdOffset);
        buffer.putInt(frequencyOffset);
        if(is_merged){          // if not merged, I don't need the following information
            buffer.putInt(docIdLength);
            buffer.putInt(frequencyLength);
            buffer.putDouble(BM25_termUpperBound);
            buffer.putDouble(TFIDF_termUpperBound);
        }else{                  // if merged, I don't need the maxDocId
            buffer.putInt(maxDocId);
        }
        buffer.flip();
        stream.write(buffer);
    }

    /*Utility function used to bring all strings to the same dimension*/
    private static byte[] stringToArrayByteFixedDim(String key) {
        byte[] byteArray = new byte[Lexicon.TERM_DIMENSION];
        byte[] inputBytes = key.getBytes(StandardCharsets.UTF_8);

        // String's bytes copy into the byte array
        System.arraycopy(inputBytes, 0, byteArray, 0, Math.min(inputBytes.length, Lexicon.TERM_DIMENSION));

        // Add padding if necessary
        for (int i = inputBytes.length; i < Lexicon.TERM_DIMENSION; i++) {
            byteArray[i] = (byte) ' ';
        }

        return byteArray;
    }

    /* ---------------------- GETTERS AND SETTERS -------------------- */
    public void updateDF(){
        df++;
    }

    public void setDocIdOffset(int docIdOffset) {
        this.docIdOffset = docIdOffset;
    }

    public void setFrequencyOffset(int frequencyOffset) {
        this.frequencyOffset = frequencyOffset;
    }


    public void setDf(int df) {
        this.df = df;
    }

    public int getDf() {
        return df;
    }

    public int getDocIdOffset() {
        return docIdOffset;
    }

    public int getFrequencyOffset() {
        return frequencyOffset;
    }

    public double getScoreTermUpperBound() {
        if(Configuration.getScoreStandard().equals("BM25"))
            return BM25_termUpperBound;
        return TFIDF_termUpperBound;
    }

    public void setTermUpperBounds(double BM25_termUpperBound, double TFIDF_termUpperBound) {
        this.BM25_termUpperBound = BM25_termUpperBound;
        this.TFIDF_termUpperBound = TFIDF_termUpperBound;
    }

    public int getMaxDocId() {
        return maxDocId;
    }

    public void setMaxId(int maxId) { this.maxDocId = maxId;}

    public void setDocIdLength(int length) { docIdLength = length;}

    public void setFrequencyLength(int length) {frequencyLength = length;}

    public int getDocIdLength() {
        return docIdLength;
    }

    public int getFrequencyLength() {
        return frequencyLength;
    }

    /* ---------------------- END GETTERS AND SETTERS -------------------- */

    @Override
    public String toString() {
        return " LexiconEntry{" +
                "df=" + df +
                ", docIdOffset=" + docIdOffset +
                ", frequencyOffset=" + frequencyOffset +
                ", termUpperBound=" + BM25_termUpperBound +
                "}\n";
    }
}


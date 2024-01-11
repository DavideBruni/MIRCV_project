package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.UnableToWriteLexiconException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import static unipi.aide.mircv.model.Lexicon.stringToArrayByteFixedDim;

public class LexiconEntry implements Serializable {
    private int df;
    private double idf;
    private int docIdOffset;
    private int frequencyOffset;
    private int docIdLength;
    private int frequencyLength;
    private int maxDocId;                    // used only during SPIMI and merge operations
    private double BM25_termUpperBound;      // max score dynamic pruning
    private double TFIDF_termUpperBound;

    public LexiconEntry(){
        df = 1;
    }

    public LexiconEntry(int df, double idf, int docIdOffset, int frequencyOffset, double BM25_termUpperBound,double TFIDF_termUpperBound, int number, boolean is_merged) {
        this.df = df;
        this.idf = idf;
        this.docIdOffset = docIdOffset;
        this.frequencyOffset = frequencyOffset;
        this.BM25_termUpperBound = BM25_termUpperBound;
        this.TFIDF_termUpperBound = TFIDF_termUpperBound;
    }

    public static int getEntryDimension(boolean is_merged) {
        return is_merged ? 44 : 16;
    }

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

    public void setIdf(double idf) {
        this.idf = idf;
    }

    public int getDf() {
        return df;
    }

    public double getIdf() {
        return idf;
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

    public double getTFIDF_termUpperBound() {
        return TFIDF_termUpperBound;
    }

    @Override
    public String toString() {
        return " LexiconEntry{" +
                "df=" + df +
                ", idf=" + idf +
                ", docIdOffset=" + docIdOffset +
                ", frequencyOffset=" + frequencyOffset +
                ", termUpperBound=" + BM25_termUpperBound +
                "}\n";
    }


    public int getMaxDocId() {
        return maxDocId;
    }

    public void setMaxId(int maxId) { this.maxDocId = maxId;}

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
        ByteBuffer buffer = ByteBuffer.allocateDirect(45+LexiconEntry.getEntryDimension(is_merged));
        buffer.put(stringToArrayByteFixedDim(key,45));
        buffer.putInt(df);
        if(is_merged)
            buffer.putDouble(idf);
        buffer.putInt(docIdOffset);
        buffer.putInt(frequencyOffset);
        if(is_merged){
            buffer.putInt(docIdLength);
            buffer.putInt(frequencyLength);
            buffer.putDouble(BM25_termUpperBound);
            buffer.putDouble(TFIDF_termUpperBound);
        }else{
            buffer.putInt(maxDocId);
        }
        buffer.flip();
        stream.write(buffer);
    }

    public void setDocIdLength(int length) { docIdLength = length;}

    public void setFrequencyLength(int length) {frequencyLength = length;}

    public int getDocIdLength() {
        return docIdLength;
    }

    public int getFrequencyLength() {
        return frequencyLength;
    }
}


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
    private int maxDocId;
    private double BM25_termUpperBound;      // max score dynamic pruning
    private double TFIDF_termUpperBound;

    public LexiconEntry(){
        df = 1;
    }

    public LexiconEntry(int df, double idf, int docIdOffset, int frequencyOffset,double BM25_termUpperBound,double TFIDF_termUpperBound, int maxDocId) {
        this.df = df;
        this.idf = idf;
        this.docIdOffset = docIdOffset;
        this.frequencyOffset = frequencyOffset;
        this.BM25_termUpperBound = BM25_termUpperBound;
        this.TFIDF_termUpperBound = TFIDF_termUpperBound;
        this.maxDocId = maxDocId;
    }

    public static int getEntryDimension() {
        return 40;
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

    public double getBM25_termUpperBound() {return BM25_termUpperBound;}

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

    public void writeToDisk(String token) throws UnableToWriteLexiconException {
        File file = new File(Configuration.getLexiconPath());
        try(FileChannel stream = (FileChannel) Files.newByteChannel(file.toPath(),
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE)){
            ByteBuffer buffer = ByteBuffer.allocateDirect(45+LexiconEntry.getEntryDimension());
            buffer.put(stringToArrayByteFixedDim(token,45));
            buffer.putInt(df);
            buffer.putDouble(idf);
            buffer.putInt(docIdOffset);
            buffer.putInt(frequencyOffset);
            buffer.putDouble(BM25_termUpperBound);
            buffer.putDouble(TFIDF_termUpperBound);
            buffer.putInt(maxDocId);
            buffer.flip();
            stream.write(buffer);
        } catch (IOException e) {
            throw new UnableToWriteLexiconException(e.getMessage());
        }
    }

}


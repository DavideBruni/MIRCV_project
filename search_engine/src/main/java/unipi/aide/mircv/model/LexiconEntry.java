package unipi.aide.mircv.model;

public class LexiconEntry {
    private int df;
    private double idf;
    private int docIdOffset;
    private int frequencyOffset;
    private int docIdSize;
    private int frequencySize;
    private int numBlocks = 1;

    public LexiconEntry(){
        df = 1;
    }

    public LexiconEntry updateDF(){
        df++;
        return this;
    }

    public void setDocIdOffset(int docIdOffset) {
        this.docIdOffset = docIdOffset;
    }

    public void setFrequencyOffset(int frequencyOffset) {
        this.frequencyOffset = frequencyOffset;
    }

    public void setNumBlocks(int numBlocks) {
        this.numBlocks = numBlocks;
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

    public int getNumBlocks() {
        return numBlocks;
    }

    public int getDocIdSize() {
        return docIdSize;
    }

    public void setDocIdSize(int docIdSize) {
        this.docIdSize = docIdSize;
    }

    public int getFrequencySize() {
        return frequencySize;
    }

    public void setFrequencySize(int frequencySize) {
        this.frequencySize = frequencySize;
    }
}


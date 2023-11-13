package unipi.aide.mircv.model;

public class LexiconEntry {
    private int df;
    private double idf;
    private int docIdOffset;
    private int frequencyOffset;
    private int postingNumber;
    private int numBlocks = 1;
    private int skipPointerOffset;
    private double termUpperBound;

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

    public int getPostingNumber() {
        return postingNumber;
    }

    public void setPostingNumber(int postingListsNumber) {
        this.postingNumber = postingListsNumber;
    }

    public void setSkipPointerOffset(int skipPointerOffset) {this.skipPointerOffset=skipPointerOffset;}

    public int getSkipPointerOffset() {return skipPointerOffset;}

    public double getTermUpperBound() {return termUpperBound;}

    public void setTermUpperBound(double termUpperBound) {this.termUpperBound = termUpperBound;}
}


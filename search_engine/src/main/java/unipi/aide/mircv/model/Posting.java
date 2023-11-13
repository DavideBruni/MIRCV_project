package unipi.aide.mircv.model;

public  class Posting{
    long docid;
    int frequency;

    public Posting(long docid, int frequency) {
        this.docid = docid;
        this.frequency = frequency;
    }

    public long getDocid() {
        return docid;
    }

    public int getFrequency() {
        return frequency;
    }
}
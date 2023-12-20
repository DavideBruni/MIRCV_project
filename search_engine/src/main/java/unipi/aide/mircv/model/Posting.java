package unipi.aide.mircv.model;

import java.io.Serializable;

public  class Posting implements Serializable {
    int docid;
    int frequency;

    public Posting(int docid, int frequency) {
        this.docid = docid;
        this.frequency = frequency;
    }

    public int getDocid() {
        return docid;
    }

    public int getFrequency() {
        return frequency;
    }

    @Override
    public String toString() {
        return "Posting{" +
                "docid=" + docid +
                ", frequency=" + frequency +
                '}';
    }
}
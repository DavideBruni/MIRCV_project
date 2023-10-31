package unipi.aide.mircv.model;

public class DocumentInfo {
    long docid;
    int docLen;

    public DocumentInfo(long docid, int docLen) {
        this.docid = docid;
        this.docLen = docLen;
    }

    public long getDocid() {
        return docid;
    }

    public int getDocLen() {
        return docLen;
    }
}

package unipi.aide.mircv.model;

public class DocumentInfo {
    long docid;
    String pid;
    int docLen;

    public DocumentInfo(String pid, long docid, int docLen) {
        this.docid = docid;
        this.docLen = docLen;
        this.pid = pid;
    }

    public long getDocid() {
        return docid;
    }

    public int getDocLen() {
        return docLen;
    }
}

package unipi.aide.mircv.model;

public class DocumentInfo {
    private long docid;
    private String pid;
    private int docLen;

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

    public void setDocid(long docid) {
        this.docid = docid;
    }

    public String getPid() {
        return pid;
    }
}

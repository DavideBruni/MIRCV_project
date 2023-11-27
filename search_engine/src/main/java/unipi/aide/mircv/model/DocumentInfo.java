package unipi.aide.mircv.model;

public class DocumentInfo {
    private int docid;
    private String pid;
    private int docLen;

    public DocumentInfo(String pid, int docid, int docLen) {
        this.docid = docid;
        this.docLen = docLen;
        this.pid = pid;
    }

    public int getDocid() {
        return docid;
    }

    public int getDocLen() {
        return docLen;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

    public String getPid() {
        return pid;
    }
}

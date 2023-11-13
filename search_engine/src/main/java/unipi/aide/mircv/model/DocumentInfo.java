package unipi.aide.mircv.model;

public class DocumentInfo {
    private long docid;
    private String pid;
    private int docLen;
    private double score;

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

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public void setDocid(long docid) {
        this.docid = docid;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public void setDocLen(int docLen) {
        this.docLen = docLen;
    }
}

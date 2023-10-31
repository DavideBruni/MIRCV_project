package unipi.aide.mircv.model;

import java.util.List;

public class ParsedDocument {
    private String pid;
    private List<String> tokens;

    public ParsedDocument(String pid, List<String> tokens) {
        this.pid = pid;
        this.tokens = tokens;
    }

    public String getPid() {
        return pid;
    }

    public List<String> getTokens() {
        return tokens;
    }
}

package unipi.aide.mircv.model;

import java.util.List;

public class ParsedDocument {
    private final String pid;
    private final List<String> tokens;

    public ParsedDocument(String pid, List<String> tokens) {
        this.pid = pid;
        this.tokens = tokens;
    }

    public List<String> getTokens() {
        return tokens;
    }
}

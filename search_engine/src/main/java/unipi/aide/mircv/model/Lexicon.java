package unipi.aide.mircv.model;

import java.util.*;

public class Lexicon {
    private static Lexicon instance = null;

    private Map<String,LexiconEntry> entries = null;

    private Lexicon(){
        entries = new HashMap<>();
    }

    public static Lexicon getInstance(){
        if (instance == null)
            instance = new Lexicon();
        return instance;
    }

    public boolean contains(String token) {
        return entries.containsKey(token);
    }

    public void add(String token) {
        entries.put(token,new LexiconEntry());
    }

    public void updateDf(String token) {
        entries.put(token, entries.get(token).updateDF());
    }

    public void updateDocIdOffset(int i) {
        // TODO
    }

    public void updateFrequencyOffset(int i) {
        // TODO
    }

    public void writeToDisk() {
        // TODO
    }


    private class LexiconEntry{
        private int df;
        private int idf;
        private long offset;
        private int numBlocks = 1;

        LexiconEntry(){
            df = 1;
        }

        LexiconEntry updateDF(){
            df++;
            return this;
        }
    }
}

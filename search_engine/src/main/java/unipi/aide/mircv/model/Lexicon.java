package unipi.aide.mircv.model;

import java.util.ArrayList;
import java.util.List;

public class Lexicon {
    private static Lexicon instance = null;

    private List<LexiconEntry> entries = null;

    private Lexicon(){
        entries = new ArrayList();
    }

    public Lexicon getInstance(){
        if (instance == null)
            instance = new Lexicon();
        return instance;
    }


    private class LexiconEntry{
        private int df;
        private int idf;

        LexiconEntry(){}
    }
}

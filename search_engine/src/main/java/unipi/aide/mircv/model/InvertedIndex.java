package unipi.aide.mircv.model;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import unipi.aide.mircv.exceptions.PidNotFoundException;
import unipi.aide.mircv.exceptions.UnableToAddDocumentIndexException;
import unipi.aide.mircv.parsing.Parser;

import java.io.*;
import java.util.*;

public class InvertedIndex {

    private static final String DOCUMENT_IDS_PATH = "data/document_ids.dat";
    private static final String FREQUENCY_PATH = "data/frequencies.dat";
    private static final int POSTING_SIZE_THRESHOLD = 2048;      //2KB
    private boolean allDocumentProcessed;
    private final long MEMORY_THRESHOLD = Runtime.getRuntime().totalMemory() * 20 / 100; // leave 20% of memory free
    private static long lastDocId = 0;

    private void SPIMI(TarArchiveInputStream tarIn, boolean parse, boolean compressed) throws IOException {
        PostingList postingList = new PostingList();
        Lexicon lexicon = new Lexicon();
        // Create a BufferedReader to read the file line by line
        BufferedReader reader = new BufferedReader(new InputStreamReader(tarIn));   // non dentro il try catch perhè la funzione è chiamata dentro un try catch, se chiudo
        // il flusso poi non posso scorrere un eventuale secondo file
        try{
            while (!allDocumentProcessed){
                while (Runtime.getRuntime().freeMemory() > MEMORY_THRESHOLD) { //build index until 80% of total memory is used
                    String line;
                    if ((line = reader.readLine()) != null) {
                        if (line.isBlank()){             // if the line read is empty, skip it
                            continue;
                        }

                        // if flag is set, remove stopwords and perform stemming
                        ParsedDocument parsedDocument = Parser.parseDocument(line, parse);

                        String pid = parsedDocument.getPid();
                        List<String> tokens = parsedDocument.getTokens();

                        int docLen = tokens.size();

                        DocumentIndex.add(++lastDocId, pid, docLen);

                        //update collection statistics
                        CollectionStatistics.updateDocumentsLen(docLen);
                        CollectionStatistics.updateCollectionSize();

                        Set<String> uniqueTerms = new HashSet<>(tokens);

                        for(String token : uniqueTerms){
                            if(! lexicon.contains(token)){
                                lexicon.add(token);
                            }else{
                                lexicon.updateDf(token);
                            }
                            postingList.add(lastDocId, token, Collections.frequency(tokens,token));
                        }
                    }else{
                        allDocumentProcessed = true;
                        break;
                        // TODO add print and log
                    }
                }
                postingList.sort();
                postingList.writeToDisk(compressed,lexicon);
                lexicon.writeToDisk(false);
                lexicon.clear();
                postingList = new PostingList();
                System.gc();
            }
        }catch (PidNotFoundException | UnableToAddDocumentIndexException e) {
            throw new RuntimeException(e);
        }

    }

    private void Merge(boolean compressed){
        // leggere più di un inverted index per volta
        //  1. Tengo aperto uno Stream per ogni Lexicon
        //  2. Cerco il token più "piccolo"
        //  2.1 Scrivi l'entrata nel nuovo Lexicon
        //  2.2 Calcola il nuovo df (somma) e idf log(N/df) dove N è il CollectionSize
        //  2.3 Recupera le varie posting list
        //  2.3.Crea la nuova posting list (docId e freqId) facendo attenzione all'ordinamento per docId (potrei buttare tutto dentro e poi ordinare)
        //  2.4 se la posting list supera 2KB, allora fai più blocchi (usa gli skipping pointers e implementali)
        //  2.5 Scrivi su file e aggiorna gli offsett
        PostingList mergedPostingList = new PostingList();
        Lexicon mergedLexicon = new Lexicon();
        try (DataOutputStream docStream = new DataOutputStream(new FileOutputStream(DOCUMENT_IDS_PATH));
             DataOutputStream freqStream = new DataOutputStream(new FileOutputStream(FREQUENCY_PATH))){
            List<Lexicon> partialLexicons= Lexicon.getPartialLexicons();

            int offset = 0;
            int [] pointers = new int[partialLexicons.size()];          // for each partialLexicon, what is the term we get
            // trovare il token minimo --> trovare i partialLexicons
            while(true) {
                LexiconEntry lexiconEntry = new LexiconEntry();
                String token = findLowerToken(partialLexicons, pointers);
                if (token == null)
                    break;
                int df = 0;
                double idf = 0;
                // recupero le posting list --> recupero le posting!
                List<PostingList> postingLists = new ArrayList<>();
                for (int i = 0; i < partialLexicons.size(); i++) {
                    if (partialLexicons.get(i).contains(token)) {   // (o meglio quali partizioni contengono minTerm)
                        df += partialLexicons.get(i).getEntry(token).getDf();
                        Lexicon tmp = partialLexicons.get(i);           // creare la nuova entry e creare la nuova posting list
                        postingLists.add(new PostingList().readFromDisk(token, i, tmp.getEntry(token).getDocIdOffset(),
                                tmp.getEntry(token).getFrequencyOffset(), tmp.getEntry(token).getDocIdSize(),
                                tmp.getEntry(token).getFrequencySize()));
                        pointers[i]++;
                    }
                }


                // creo un'unica posting list
                int postingsSize = mergedPostingList.add(postingLists, token);
                //gestire la situazione blocchi
                if (postingsSize > POSTING_SIZE_THRESHOLD){
                    mergedPostingList.addSkipPointers(token,lexiconEntry,compressed); //if some error, it must return 1
                }

                // aggiorna l'entry del vocabolario
                idf = Math.log(CollectionStatistics.getCollectionSize() / (double) df);

                lexiconEntry.setDf(df);
                lexiconEntry.setIdf(idf);
                // scrivere in docId, in frequency e lexicon
                if (!compressed) {
                    offset= mergedPostingList.writeToDiskNotCompressed(mergedLexicon,docStream,freqStream,token,offset);
                }
                mergedLexicon.add(token, lexiconEntry);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        mergedLexicon.writeToDisk(true);

    }

    private String findLowerToken(List<Lexicon> partialLexicons, int [] pointers) throws IOException {
        String minTerm = null;
        for(int i=0; i< partialLexicons.size(); i++){
            // gestire il caso in cui il pointer sia OutOfBoundExceptions
            String currentTerm = partialLexicons.get(i).getEntryAtPointer(pointers[i]);
            if (minTerm == null)
               minTerm=currentTerm;
            else if (currentTerm == null) {
                continue;
            } else if(currentTerm.compareTo(minTerm) < 0)
                minTerm=currentTerm;
        }
        return minTerm;
    }


    public InvertedIndex(TarArchiveInputStream tarArchiveInputStream, boolean parse, boolean compressed) throws IOException {
        SPIMI(tarArchiveInputStream, parse, compressed);
        if(!allDocumentProcessed){
            //fai qualcosa
        }else{
            Merge(compressed);
        }
        CollectionStatistics.writeToDisk();
    }
}

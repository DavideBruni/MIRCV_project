package unipi.aide.mircv.model;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import unipi.aide.mircv.exceptions.PidNotFoundException;
import unipi.aide.mircv.exceptions.UnableToAddDocumentIndexException;
import unipi.aide.mircv.parsing.Parser;

import java.io.*;
import java.util.*;

public class InvertedIndex {

    private static final String DOCUMENT_IDS_PATH = "data/invertedIndex/document_ids.dat";
    private static final String FREQUENCY_PATH = "data/invertedIndex/frequencies.dat";

    private boolean allDocumentProcessed;
    private final long MEMORY_THRESHOLD = Runtime.getRuntime().totalMemory() * 20 / 100; // leave 20% of memory free
    private static long lastDocId = 0;

    private void SPIMI(TarArchiveInputStream tarIn, boolean parse, boolean compressed) throws IOException {
        PostingLists postingLists = new PostingLists();
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
                            postingLists.add(lastDocId, token, Collections.frequency(tokens,token));
                        }
                    }else{
                        allDocumentProcessed = true;
                        break;
                        // TODO add print and log
                    }
                }
                postingLists.sort();
                postingLists.writeToDisk(compressed,lexicon);
                lexicon.writeToDisk(false);
                lexicon.clear();
                postingLists = new PostingLists();
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
        PostingLists mergedPostingLists = new PostingLists();
        Lexicon mergedLexicon = new Lexicon();
        DataInputStream[] partialLexiconStreams = Lexicon.getStreams();
        String[] lowestTokens = Lexicon.getFirstTokens(partialLexiconStreams);
        try (FileOutputStream docStream = new FileOutputStream(DOCUMENT_IDS_PATH);
             FileOutputStream freqStream = new FileOutputStream(FREQUENCY_PATH)){
            // leggo per tutti i partial, solo la prima entrata e la salvo in un array di LExiconEntries
            // recupero il token minore
            // merge: quello che faccio ora + rimuovo dall'array quelli già analizzati
            // fine ciclo
            // per gli indici dove il valore della entry è null, leggo una nuova entry  (nella lettura gestire il case EOF e lasciare a null)
            // (nel confronto gestire il null pointer exception quando accedo al token dell'entry)
            int offset = 0;
            int compressed_offset [] = new int[]{0,0};
            // trovare il token minimo --> trovare i partialLexicons
            while(true) {
                LexiconEntry lexiconEntry = new LexiconEntry();
                String token = findLowerToken(lowestTokens);
                if (token == null)
                    break;
                int df = 0;
                double idf;
                // recupero le posting list --> recupero le posting!
                List<PostingLists> postingLists = new ArrayList<>();
                for (int i = 0; i < lowestTokens.length; i++) {
                    if (lowestTokens[i].equals(token)) {   // quali partizioni contengono minTerm)
                        LexiconEntry tmp = Lexicon.readEntry(partialLexiconStreams[i]);
                        if(tmp != null) {
                            df += tmp.getDf();
                            postingLists.add(new PostingLists().readFromDisk(token, i, tmp.getDocIdOffset(),
                                    tmp.getFrequencyOffset(),tmp.getPostingNumber(),compressed));
                            lowestTokens[i] = null;
                        }
                    }
                }
                // creo un'unica posting list
                mergedPostingLists.add(postingLists, token);
                // aggiorna l'entry del vocabolario
                idf = Math.log(CollectionStatistics.getCollectionSize() / (double) df);
                lexiconEntry.setDf(df);
                lexiconEntry.setIdf(idf);
                // scrivere in docId, in frequency e lexicon
                if (!compressed) {
                    DataOutputStream dos_docStream = new DataOutputStream(docStream);
                    DataOutputStream dos_freqStream = new DataOutputStream(freqStream);
                    offset = mergedPostingLists.writeToDiskNotCompressed(mergedLexicon,dos_docStream,dos_freqStream,offset, true);
                }else{
                    compressed_offset = mergedPostingLists.writeToDiskCompressed(mergedLexicon,docStream,freqStream,compressed_offset[0],compressed_offset[0],true);

                }
                mergedLexicon.add(token, lexiconEntry);
                mergedPostingLists = new PostingLists();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        mergedLexicon.writeToDisk(true);

    }

    private String findLowerToken(String[] tokens){
        String minTerm = null;
        for(String token : tokens){
            if (token != null) {
                if (minTerm == null)
                    minTerm = token;
                else if (token.compareTo(minTerm) < 0)
                    minTerm = token;
            }
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

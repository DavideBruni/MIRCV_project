package unipi.aide.mircv.model;

import unipi.aide.mircv.exceptions.PidNotFoundException;
import unipi.aide.mircv.parsing.Parser;

import java.io.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InvertedIndex {

    private boolean allDocumentProcessed;
    private final long MEMORY_THRESHOLD = Runtime.getRuntime().totalMemory() * 20 / 100; // leave 20% of memory free
    private static long lastDocId = 0;

    private boolean SPIMI(File tsvFile, boolean parse, boolean compressed){
        PostingList postingList = new PostingList();
        Lexicon lexicon = Lexicon.getInstance();
        boolean allDocumentProcessed = false;
        while (Runtime.getRuntime().freeMemory() > MEMORY_THRESHOLD) { //build index until 80% of total memory is used
            // Create a BufferedReader to read the file line by line
            try (BufferedReader reader = new BufferedReader(new FileReader(tsvFile))) {
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

                    // TODO DocumentIndex.add(++lastDocId, pid, docLen);

                    //update collection statistics
                    CollectionStatistics.updateDocumentsLen(docLen);

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
            } catch (IOException e) {
                e.printStackTrace();
            } catch (PidNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        postingList.sort();
        postingList.writeToDisk(compressed);
        lexicon.writeToDisk();

        return allDocumentProcessed;
    }

    private void Merge(){

    }


    public void createIndex(File tsvFile, boolean parse, boolean compressed) {
        while (! allDocumentProcessed) {
            allDocumentProcessed = SPIMI(tsvFile, parse, compressed);
        }
        // write CollectionStatistics to the disk
    }
}

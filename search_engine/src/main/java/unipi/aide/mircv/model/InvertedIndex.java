package unipi.aide.mircv.model;

import unipi.aide.mircv.exceptions.UnableToAddDocumentIndexExcpetion;
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

    private boolean SPIMI(File tsvFile, boolean parse){
        PostingList postingList = new PostingList();
        Lexicon lexicon = Lexicon.getInstance();
        while (Runtime.getRuntime().freeMemory() > MEMORY_THRESHOLD) { //build index until 80% of total memory is used
            // Create a BufferedReader to read the file line by line
            try (BufferedReader reader = new BufferedReader(new FileReader(tsvFile))) {
                String line;
                if ((line = reader.readLine()) != null) {
                    if (line.isBlank()){             // if the line read is empty, skip it
                        continue;
                    }

                    // TODO converti le getPid e getTokens in ParseDocument che restituisce un ParsedDocument

                    // getting the pid of the document
                    String pid = Parser.getPid(line);

                    // if flag is set, remove stopwords and perform stemming
                    List<String> tokens = Parser.getTokens(line,parse);

                    int docLen = tokens.size();

                    DocumentIndex.add(++lastDocId, pid, docLen);

                    //update collection statistics
                    CollectionStatistics.updateDocumentsLen(docLen);

                    Set<String> uniqueTerms = new HashSet<>(tokens);

                    for(String token : uniqueTerms){
                        if(! lexicon.contains(token)){
                            lexicon.add(token);
                        }else{
                            lexicon.update(token);
                        }
                        postingList.add(pid, token, Collections.frequency(tokens,token));
                    }
                }else{
                    return true;
                    // TODO add print and log
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (UnableToAddDocumentIndexExcpetion e) {
                // print log
                lastDocId--;

            }
        }
        // sort dictionary

        return false;
    }

    private void Merge(){

    }


    public void createIndex(File tsvFile, boolean parse) {
        while (! allDocumentProcessed) {
            allDocumentProcessed = SPIMI(tsvFile, parse);
        }
        // write CollectionStatistics to the disk
    }
}

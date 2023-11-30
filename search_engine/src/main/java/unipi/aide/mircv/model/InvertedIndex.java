package unipi.aide.mircv.model;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.*;
import unipi.aide.mircv.helpers.StreamHelper;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.parsing.Parser;
import unipi.aide.mircv.queryProcessor.Scorer;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class InvertedIndex {

    private static boolean allDocumentProcessed;
    private static int lastDocId = 0;

    /***
     *
     * @param tarIn         TarInputStream from which read the collection
     * @param parse         indicate if perform stemming and stopwords filtering
     */
    private static void SPIMI(TarArchiveInputStream tarIn, boolean parse, boolean debug) throws IOException {
        //customLogger.info("Start SPIMI algorithm");
        PostingLists postingLists = new PostingLists();
        Lexicon lexicon = Lexicon.getInstance();
        // Create a BufferedReader to read the file line by line
        BufferedReader reader = new BufferedReader(new InputStreamReader(tarIn));   // since the function is called inside another try catch, I cannot do this inside the try catch with resource, because I don't have to close the stream
        try{
            while (!allDocumentProcessed){
                boolean freeMemory = true;
                while (Runtime.getRuntime().freeMemory() > (Runtime.getRuntime().totalMemory() * 20 / 100)) {      // build index until 80% of total memory is used
                    freeMemory = false;
                    //customLogger.info("Reading new document...");
                    String line;
                    if ((line = reader.readLine()) != null) {
                        if (line.isBlank()){             // if the line read is empty, skip it
                            //customLogger.info("Document is empty, skipped");
                            continue;
                        }

                        ParsedDocument parsedDocument;
                        //customLogger.info("Parsing document (candidate id "+lastDocId+"), searching PID and getting tokens");
                        try {
                            // if flag is set, remove stopwords and perform stemming
                            parsedDocument = Parser.parseDocument(line, parse);
                        }catch (PidNotFoundException pe){
                            CustomLogger.error("PID not found for the current document, skip it");
                            continue;
                        }
                        String pid = parsedDocument.getPid();

                        List<String> tokens = parsedDocument.getTokens();

                        int docLen = tokens.size();

                        // adding documentLen (number of tokens) and mapping between docno and docId to documentIndex.
                        DocumentIndex.add(++lastDocId, pid, docLen);

                        //update collection statistics
                        CollectionStatistics.updateDocumentsLen(docLen);
                        CollectionStatistics.updateCollectionSize();

                        Set<String> uniqueTerms = new HashSet<>(tokens);
                        for(String token : uniqueTerms){
                            // if missing, a new entry will be added
                            lexicon.updateDfAndNumberOfPosting(token);        // if yes, we update the documentFrequency of the term
                            // create a new Posting and add it to the postingList of the term "token"
                            postingLists.add(lastDocId, token, Collections.frequency(tokens,token));
                        }
                    }else{
                        allDocumentProcessed = true;
                        //customLogger.info("All document have been processed");
                        break;
                    }
                }
                if(!freeMemory) {
                    /* necessary because sometimes the while condition remain false for a certain amount of time, so I don't want
                        to write empty index on disk */
                    //customLogger.info("Sorting posting lists generated (last document id was " + lastDocId + ") and write them on disk");
                    postingLists.sort();
                    try {
                        //customLogger.info("Writing lexicon on disk");
                        postingLists.writeToDisk(Configuration.isCOMPRESSED());
                    } catch (PostingListStoreException pe) {
                        break;
                    }

                    if (debug) {
                        System.out.println(Lexicon.getInstance());
                        System.out.println(postingLists);
                        CollectionStatistics.print();
                    }

                    Lexicon.writeToDisk(false, debug);
                    Lexicon.clear();
                    postingLists = new PostingLists();
                }
                Runtime.getRuntime().gc();
            }
            DocumentIndex.closeStreams();
        } catch (UnableToWriteLexiconException e) {
            CustomLogger.error("Error while storing the Lexicon. Aborting index creation");
        } catch (UnableToAddDocumentIndexException e) {
            CustomLogger.error("Error while storing the DocumentIndex. Aborting index creation");
        }

    }

    private static void Merge(boolean debug) {
        //customLogger.info("Starting merging operation...");
        PostingLists mergedPostingLists = new PostingLists();
        // 1. I have to keep open one stream for each lexicon partition
        DataInputStream[] partialLexiconStreams = Lexicon.getStreams();
        PriorityQueue<LexiconEntry> lexiconCache = new PriorityQueue<>(Comparator.comparingInt(LexiconEntry::getDf));
        String[] lowestTokens;
        try {
            // 2. In order to find the "lowest" token, I read the smallest token for each stream, in this case, the firsts
            lowestTokens = Lexicon.getFirstTokens(partialLexiconStreams);
        }catch (PartialLexiconNotFoundException e ){
            CustomLogger.error("Error in reading partial lexicons, aborting merging...");
            closeStreams(partialLexiconStreams);
            return;
        }
        try (FileOutputStream docStream = new FileOutputStream(Configuration.getDocumentIdsPath());
             FileOutputStream freqStream = new FileOutputStream(Configuration.getFrequencyPath())){
            DataOutputStream dos_docStream = null;
            DataOutputStream dos_freqStream = null;
            if(!Configuration.isCOMPRESSED()){
                dos_docStream = new DataOutputStream(docStream);
                dos_freqStream = new DataOutputStream(freqStream);
            }
            int offset = 0;
            int[] compressed_offset = new int[]{0,0};
            while(true) {
                String token = findLowerToken(lowestTokens);
                // if there's not a lower token, I merged them all
                if (token == null)
                    break;
                LexiconEntry lexiconEntry = new LexiconEntry();
                int df = 0;
                double idf;
                List<PostingList> postingLists = new ArrayList<>();
                // CustomLogger.info("Retrieving all the posting list for token '" + token +"'");
                for (int i = 0; i < lowestTokens.length; i++) {
                    if (lowestTokens[i] != null && lowestTokens[i].equals(token)) {
                        // I consider only partitions that have the lowest token (they could be more than 1)
                        LexiconEntry tmp = Lexicon.readEntry(partialLexiconStreams[i]);
                        if(tmp != null) {
                            df += tmp.getDf();
                            try {
                                // if the term is present, I have to read the posting list from disk and accumulate them in a List of Posting list
                                postingLists.add(new PostingList().readFromDisk(token, i, tmp.getDocIdOffset(),
                                        tmp.getFrequencyOffset(), tmp.getPostingNumber(), Configuration.isCOMPRESSED()));
                            }catch (NullPointerException npe){
                                continue;
                            }
                            lowestTokens[i] = null;     //for this partition, I have to search another token
                            lexiconEntry.updateNumberOfPostings(tmp.getPostingNumber());
                        }
                    }
                }
                //customLogger.info("Merging posting lists together");
                PostingList mergedPostingList = PostingLists.merge(postingLists, token);
                // Update lexicon entry
                idf = Math.log(CollectionStatistics.getCollectionSize() / (double) df);
                lexiconEntry.setDf(df);
                lexiconEntry.setIdf(idf);

                CollectionStatistics.setLongestTerm(token.length());

                Lexicon.getInstance().add(token, lexiconEntry);
                // I have to store the merged posting list on disk
                if (!Configuration.isCOMPRESSED()) {
                    offset = mergedPostingList.writeToDiskNotCompressed(dos_docStream,dos_freqStream,offset, true, lexiconEntry);
                }else{
                    compressed_offset = mergedPostingList.writeToDiskCompressed(docStream,freqStream,compressed_offset[0],compressed_offset[1],true,lexiconEntry);
                }

                if(debug){
                    System.out.println(mergedPostingList);
                }

                Lexicon.getInstance().add(token, lexiconEntry);

                if(Lexicon.getInstance().numberOfEntries() >= 30){     //28 byte + dim parola
                    CollectionStatistics.updateNumberOfToken(Lexicon.getInstance().numberOfEntries());  //30
                    if(debug)
                        System.out.println(Lexicon.getInstance());
                    // every 30 entries write to disk, it's better than write every entry
                    Lexicon.writeToDisk(true,debug);
                    Lexicon.clear();
                }
                Lexicon.getTokens(lowestTokens, partialLexiconStreams);
            }

            if(!Configuration.isCOMPRESSED()){
                dos_docStream.close();
                dos_freqStream.close();
            }

        } catch (IOException | PartialLexiconNotFoundException | UnableToWriteLexiconException e) {
            CustomLogger.error("Unable to merge Partial invertedIndex "+e);
            e.printStackTrace();
        }

        try {
            // since I write every 30 entries, if I don't have a number multiple of 30 I might not write a part of the lexicon
            // anyway if the lexicon is empty, the writeToDisk function handle it and do nothing
            if(debug)
                System.out.println(Lexicon.getInstance());
            Lexicon.writeToDisk(true,debug);
            CollectionStatistics.updateNumberOfToken(Lexicon.getInstance().numberOfEntries());
            Lexicon.clear();
        } catch (UnableToWriteLexiconException e) {
            throw new RuntimeException(e);
        }
        closeStreams(partialLexiconStreams);
        DocumentIndex.closeStreams();
        if(debug)
            CollectionStatistics.print();
    }

    private static void closeStreams(InputStream[] partialLexiconStreams) {
        for(InputStream stream : partialLexiconStreams){
            try {
                stream.close();
            } catch (IOException e) {
                CustomLogger.error("Unable to close stream "+ e.getMessage());
            }
        }
    }

    /**
     * Finds and returns the lexicographically smallest non-null token from the given array of tokens.
     * If the array is empty or contains only null elements, the method returns null.
     *
     * @param tokens An array of tokens to search for the lexicographically smallest non-null token.
     * @return The lexicographically smallest non-null token, or null if the array is empty or contains only null elements.
     */
    private static String findLowerToken(String[] tokens){
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



    /**
     * Creates an inverted index from a TarArchiveInputStream, using the SPIMI algorithm.
     * The method processes the input stream, merges intermediate indices, and writes the final inverted index to disk.
     *
     * @param tarArchiveInputStream The input stream containing Tar archives of documents.
     * @param parse                A boolean flag indicating whether to perform document parsing during the SPIMI phase.
     */
    public static void createInvertedIndex(TarArchiveInputStream tarArchiveInputStream, boolean parse, boolean debug) throws IOException {
       SPIMI(tarArchiveInputStream, parse, debug);
       CollectionStatistics.writeToDisk();
       if(!allDocumentProcessed){
            CustomLogger.error("Index Creation aborted, read the log to find the cause");
       }else{
            Merge(debug);
            CollectionStatistics.writeToDisk();
       }
       StreamHelper.deleteDir(Paths.get(Configuration.getRootDirectory(), "invertedIndex", "temp"));
    }


}

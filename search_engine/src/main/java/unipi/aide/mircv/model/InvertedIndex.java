package unipi.aide.mircv.model;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.*;
import unipi.aide.mircv.helpers.StreamHelper;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.parsing.Parser;
import unipi.aide.mircv.queryProcessor.Scorer;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class InvertedIndex {

    private static boolean allDocumentProcessed;
    private static int lastDocId = 0;

    /***
     *
     * @param tarIn         TarInputStream from which read the collection
     * @param parse         indicate if perform stemming and stopwords filtering
     * @param debug         indicate if print debug information
     */
    private static void SPIMI(TarArchiveInputStream tarIn, boolean parse, boolean debug) throws IOException {
        PostingLists postingLists = new PostingLists();
        Lexicon lexicon = Lexicon.getInstance();
        // since the function is called inside another try catch,
        // I cannot do this inside the try catch with resource, because I don't have to close the stream
        BufferedReader reader = new BufferedReader(new InputStreamReader(tarIn));
        try{
            while (!allDocumentProcessed){
                boolean freeMemory = true;      // see the if(!freeMemory) explanation
                // build index until 80% of total memory is used
                while (Runtime.getRuntime().freeMemory() > (Runtime.getRuntime().totalMemory() * 20 / 100)) {
                    freeMemory = false;
                    String line;
                    if ((line = reader.readLine()) != null) {
                        /* ---------------- DOCUMENT PARSING AND TOKENIZATION -------------- */
                        if (line.isBlank()){             // if the line read is empty, skip it
                            continue;
                        }
                        ParsedDocument parsedDocument;
                        try {
                            parsedDocument = Parser.parseDocument(line, parse);
                            lastDocId++;
                        }catch (PidNotFoundException pe){
                            CustomLogger.error("PID not found for the current document, skip it");
                            continue;
                        }catch (UnsupportedEncodingException ue){
                            CustomLogger.error("The document is not UTF-8 encoded");
                            continue;
                        }
                        List<String> tokens = parsedDocument.getTokens();
                        /* ---------------- END DOCUMENT PARSING AND TOKENIZATION -------------- */

                        /* ---------------- SET UP DOCUMENT INDEX AND COLLECTION STATISTICS -------------- */
                        int docLen = tokens.size();
                        // adding documentLen (number of tokens)
                        // Note: docno = docId, since all the docNo in our collection are integers from 1 to 8Milions
                        DocumentIndex.add(docLen);
                        //update collection statistics
                        CollectionStatistics.updateDocumentsLen(docLen);
                        CollectionStatistics.updateCollectionSize();
                        /* ---------------- END SET UP DOCUMENT INDEX AND COLLECTION STATISTICS -------------- */
                        /* ---------------- POSTING LISTS CREATION FOR SINGLE DOCUMENT -------------- */
                        Set<String> uniqueTerms = new HashSet<>(tokens);
                        for(String token : uniqueTerms){
                            lexicon.updateDf(token);
                            // create a new Posting and add it to the postingList of the term "token"
                            postingLists.add(lastDocId, token, Collections.frequency(tokens,token));
                        }
                        /* ---------------- END POSTING LISTS CREATION FOR SINGLE DOCUMENT -------------- */
                    }else{
                        allDocumentProcessed = true;
                        break;
                    }
                }
                /* ---------------- WRITING ON DISK -------------- */
                if(!freeMemory) {
                    /* necessary because sometimes the while condition remain false for a certain amount of time, so I don't want
                        to write empty index on disk */
                    postingLists.sort();
                    try {
                        postingLists.writeOnDisk(Configuration.isCOMPRESSED());
                    } catch (PostingListStoreException pe) {
                        break;
                    }
                    if (debug) {
                        System.out.println(Lexicon.getInstance());
                        System.out.println(postingLists);
                        CollectionStatistics.print();
                    }
                    Lexicon.writeOnDisk(false, debug);
                    Lexicon.clear();            //After writing a block, I need a brand-new Index (Lexicon and postingLists)
                    postingLists = new PostingLists();
                }
                /* ---------------- END WRITING ON DISK -------------- */
                Runtime.getRuntime().gc();      //call the garbage collector in order to free unused memory
            }
            DocumentIndex.writeOnDisk();
        } catch (UnableToWriteLexiconException e) {
            CustomLogger.error("Error while storing the Lexicon. Aborting index creation");
        } catch (UnableToWriteDocumentIndexException e) {
            CustomLogger.error("Error while storing the DocumentIndex. Aborting index creation");
        }

    }

    private static void Merge(boolean debug) {
        /* ------------------- INITIALIZATION---------------------- */
        // 1. I have to keep open one stream for each lexicon partition
        FileChannel[] partialLexiconStreams = Lexicon.getStreams();
        String[] lowestTokens;
        try {
            // 2. In order to find the "lowest" token, I read the smallest token for each stream, in this case, the firsts
            lowestTokens = Lexicon.getFirstTokens(partialLexiconStreams);
        }catch (PartialLexiconNotFoundException e ){
            CustomLogger.error("Error in reading partial lexicons, aborting merging...");
            closeStreams(partialLexiconStreams);
            return;
        }
        /* ------------------- END INITIALIZATION---------------------- */
        try(FileChannel docStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getDocumentIdsPath()),
                                                                                StandardOpenOption.WRITE,
                                                                                StandardOpenOption.READ,
                                                                                StandardOpenOption.CREATE);
            FileChannel freqStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getFrequencyPath()),
                                                                                StandardOpenOption.WRITE,
                                                                                StandardOpenOption.READ,
                                                                                StandardOpenOption.CREATE)){
            int[] offsets = new int[]{0,0};
            LexiconEntry[] partialLexiconEntries = new LexiconEntry[partialLexiconStreams.length];
            // each element of partialLexiconEntries will be equal to the lexiconEntry of the lowest term for that partition
            while(true) {
                String token = findLowerToken(lowestTokens);
                if (token == null)  // if there's not a lower token, I merged them all
                    break;
                /* --------- The lexiconEntry and all other information related to the postingList merged -------- */
                LexiconEntry lexiconEntry = new LexiconEntry();
                int df = 0;
                double idf;
                double BM25_ub = 0.0;
                double TFIDF_ub = 0.0;
                int maxDocId = 0;
                /* ---------------------------------------------- End ---------------------------------------------*/
                int [] start_positions = offsets.clone();
                for (int i = 0; i < lowestTokens.length; i++) {
                    // I consider only partitions that have the lowest token (they could be more than 1)
                    // I need to check if != null because at the end a lot of elements will be null
                    if (lowestTokens[i] != null && lowestTokens[i].equals(token)) {
                        LexiconEntry tmp = Lexicon.readEntryFromPartition(partialLexiconStreams[i]);
                        if (tmp != null) {
                            df += tmp.getDf();
                            partialLexiconEntries[i] = tmp;
                            maxDocId = tmp.getMaxDocId();
                        }
                    }
                }
                /* ------------ UPDATE LEXICON ENTRY ----------- */
                idf = Math.log10(CollectionStatistics.getCollectionSize() / (double) df);
                lexiconEntry.setDf(df);
                lexiconEntry.setDocIdOffset(start_positions[0]);
                lexiconEntry.setFrequencyOffset(start_positions[1]);
                /* --------------------------------------- */
                double[] scores;
                UncompressedPostingList notWrittenYetPostings = new UncompressedPostingList();
                /* The strategy is the following:
                    - get the postingList from the first partitions having the lowest token (partition are ordered, so previous
                        partitions have smaller documentIds that must be written first
                    - accumulate until I reached the dimension of a block at least, than write the first block(s)
                    - if some posting still remaining, write the last block with a different number of elements
                 */
                for (int i = 0; i < partialLexiconEntries.length; i++) {
                    if (lowestTokens[i] != null && lowestTokens[i].equals(token)) {     // again, only lexiconEntry with token == lowestToken
                        LexiconEntry tmp = partialLexiconEntries[i];
                        // read the posting list and decompress if necessary
                        UncompressedPostingList partialPostingList = PostingList.readFromDisk(i, tmp.getDocIdOffset(),
                                tmp.getFrequencyOffset(), Configuration.isCOMPRESSED());
                        offsets = partialPostingList.writeToDiskMerged(docStream, freqStream, offsets, df, notWrittenYetPostings, maxDocId);
                        scores = Scorer.calculateTermUpperBounds(partialPostingList, idf);
                        BM25_ub = Math.max(BM25_ub, scores[0]);
                        TFIDF_ub = Math.max(TFIDF_ub, scores[1]);
                        lowestTokens[i] = null;
                    }
                }
                offsets = notWrittenYetPostings.writeToDiskMerged(docStream, freqStream, offsets, df, null, maxDocId);
                scores = Scorer.calculateTermUpperBounds(notWrittenYetPostings, idf);
                BM25_ub = Math.max(BM25_ub, scores[0]);
                TFIDF_ub = Math.max(TFIDF_ub, scores[1]);
                // update lexiconEntry stats: length are necessary in order to load all the posting list in memory when performing
                // query processing
                lexiconEntry.setDocIdLength(offsets[0] - start_positions[0]);
                lexiconEntry.setFrequencyLength(offsets[1] - start_positions[1]);
                // term upper bounds are necessary for dynamic pruning
                lexiconEntry.setTermUpperBounds(BM25_ub, TFIDF_ub);
                lexiconEntry.writeOnDisk(token);
                CollectionStatistics.updateNumberOfToken(1);
                Lexicon.getTokens(lowestTokens, partialLexiconStreams);
            }
        } catch (IOException | PartialLexiconNotFoundException | UnableToWriteLexiconException e) {
            CustomLogger.error("Unable to merge Partial invertedIndex "+e);
            e.printStackTrace();
        }

        closeStreams(partialLexiconStreams);
        if(debug)
            CollectionStatistics.print();
    }

    private static void closeStreams(FileChannel[] partialLexiconStreams) {
        for(FileChannel stream : partialLexiconStreams){
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
        if(!allDocumentProcessed){
            CustomLogger.error("Index Creation aborted, read the log to find the cause");
        }else{

        Merge(debug);
        CollectionStatistics.writeToDisk();
       }
      StreamHelper.deleteDir(Paths.get(Configuration.getRootDirectory(), "invertedIndex", "temp"));

    }

}

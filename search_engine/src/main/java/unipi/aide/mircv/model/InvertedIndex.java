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
     */
    private static void SPIMI(TarArchiveInputStream tarIn, boolean parse, boolean debug) throws IOException {
        PostingLists postingLists = new PostingLists();
        Lexicon lexicon = Lexicon.getInstance();
        BufferedReader reader = new BufferedReader(new InputStreamReader(tarIn));   // since the function is called inside another try catch, I cannot do this inside the try catch with resource, because I don't have to close the stream
        try{
            while (!allDocumentProcessed){
                boolean freeMemory = true;
                while (Runtime.getRuntime().freeMemory() > (Runtime.getRuntime().totalMemory() * 20 / 100)) {      // build index until 80% of total memory is used
                    freeMemory = false;
                    String line;
                    if ((line = reader.readLine()) != null) {
                        if (line.isBlank()){             // if the line read is empty, skip it
                            continue;
                        }
                        ParsedDocument parsedDocument;
                        try {
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
                            lexicon.updateDf(token);        // if yes, we update the documentFrequency of the term
                            // create a new Posting and add it to the postingList of the term "token"
                            postingLists.add(lastDocId, token, Collections.frequency(tokens,token));
                        }
                    }else{
                        allDocumentProcessed = true;
                        break;
                    }
                }
                if(!freeMemory) {
                    /* necessary because sometimes the while condition remain false for a certain amount of time, so I don't want
                        to write empty index on disk */
                    postingLists.sort();
                    try {
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
        try(FileChannel docStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getDocumentIdsPath()),
                                                                                StandardOpenOption.WRITE,
                                                                                StandardOpenOption.READ,
                                                                                StandardOpenOption.CREATE);
            FileChannel freqStream = (FileChannel) Files.newByteChannel(Path.of(Configuration.getFrequencyPath()),
                                                                                StandardOpenOption.WRITE,
                                                                                StandardOpenOption.READ,
                                                                                StandardOpenOption.CREATE)){
            int offset = 0;
            int[] compressed_offset = new int[]{0,0};
            while(true) {
                String token = findLowerToken(lowestTokens);
                // if there's not a lower token, I merged them all
                if (token == null)
                    break;
                LexiconEntry lexiconEntry = new LexiconEntry();
                LexiconEntry[] partialLexiconEntries = new LexiconEntry[partialLexiconStreams.length];
                int df = 0;
                double idf;
                double BM25_ub = 0.0;
                double TFIDF_ub = 0.0;
                int maxDocId = 0;
                for (int i = 0; i < lowestTokens.length; i++) {
                    if (lowestTokens[i] != null && lowestTokens[i].equals(token)) {
                        // I consider only partitions that have the lowest token (they could be more than 1)
                        LexiconEntry tmp = Lexicon.readEntry(partialLexiconStreams[i]);
                        if(tmp != null) {
                            df += tmp.getDf();
                            partialLexiconEntries[i] = tmp;
                            maxDocId = tmp.getMaxDocId();
                        }

                    }
                }
                lexiconEntry.setDf(df);
                idf = Math.log(CollectionStatistics.getCollectionSize() / (double) df);
                lexiconEntry.setIdf(idf);
                if(Configuration.isCOMPRESSED()){
                    lexiconEntry.setDocIdOffset(compressed_offset[0]);
                    lexiconEntry.setFrequencyOffset(compressed_offset[1]);
                    double [] scores;
                    int compressedSize = EliasFano.getCompressedSize(maxDocId,df);
                    byte [] compressedId = new byte[compressedSize];
                    final int l = EliasFano.getL(maxDocId, df);
                    long lowBitsOffset = 0;
                    long highBitsOffset = EliasFano.roundUp(l * df, Byte.SIZE);
                    long [] docIdsOffset = new long[]{lowBitsOffset,highBitsOffset};
                    int prev = 0;
                    for(int i = 0; i<partialLexiconEntries.length;i++){
                        if (lowestTokens[i] != null && lowestTokens[i].equals(token)) {
                            LexiconEntry tmp = partialLexiconEntries[i];
                            PostingList partialPostingList = new PostingList().readFromDisk(token, i, tmp.getDocIdOffset(),
                                    tmp.getFrequencyOffset(), tmp.getDf(), tmp.getMaxDocId(), Configuration.isCOMPRESSED());
                            prev = EliasFano.compress(partialPostingList.docIds, compressedId, l, docIdsOffset,prev);
                            List<BitSet> compressedFrequencies = UnaryCompressor.compress(partialPostingList.frequencies);
                            compressed_offset[1] = UnaryCompressor.writeToDisk(compressedFrequencies, freqStream, compressed_offset[1]);
                            scores = Scorer.calculateTermUpperBounds(partialPostingList, idf);
                            BM25_ub = Math.max(BM25_ub, scores[0]);
                            TFIDF_ub = Math.max(TFIDF_ub, scores[1]);
                            lowestTokens[i] = null;
                        }
                    }
                    compressed_offset[0]+= EliasFano.writeToDisk(compressedId,docStream);
                }else{
                    lexiconEntry.setDocIdOffset(offset * Integer.BYTES);
                    lexiconEntry.setFrequencyOffset(offset * Integer.BYTES);
                    double [] scores;

                    for(int i = 0; i<partialLexiconEntries.length;i++){
                        if (lowestTokens[i] != null && lowestTokens[i].equals(token)) {
                            LexiconEntry tmp = partialLexiconEntries[i];
                            PostingList partialPostingList = new PostingList().readFromDisk(token, i, tmp.getDocIdOffset(),
                                    tmp.getFrequencyOffset(), tmp.getDf(), tmp.getMaxDocId(), Configuration.isCOMPRESSED());
                            offset = partialPostingList.writeToDiskNotCompressed(docStream, freqStream, offset);
                            // manca la scrittura su disco
                            scores = Scorer.calculateTermUpperBounds(partialPostingList, idf);
                            BM25_ub = Math.max(BM25_ub, scores[0]);
                            TFIDF_ub = Math.max(TFIDF_ub, scores[1]);
                            maxDocId = partialPostingList.getDocIds().get(partialPostingList.getDocIds().size() - 1);
                            lowestTokens[i] = null;
                        }
                    }
                }
                lexiconEntry.setMaxId(maxDocId);
                lexiconEntry.setTermUpperBounds(BM25_ub,TFIDF_ub);
                lexiconEntry.writeToDisk(token);
                CollectionStatistics.updateNumberOfToken(1);
                Lexicon.getTokens(lowestTokens, partialLexiconStreams);
            }

        } catch (IOException | PartialLexiconNotFoundException | UnableToWriteLexiconException e) {
            CustomLogger.error("Unable to merge Partial invertedIndex "+e);
            e.printStackTrace();
        }

        closeStreams(partialLexiconStreams);
        DocumentIndex.closeStreams();
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
        CollectionStatistics.writeToDisk();

        if(!allDocumentProcessed){
            CustomLogger.error("Index Creation aborted, read the log to find the cause");
        }else{

 /*
        try {
            CollectionStatistics.readFromDisk();
        } catch (MissingCollectionStatisticException e) {
            throw new RuntimeException(e);
        }
        Lexicon lexicon = Lexicon.getInstance();

 */

        Merge(debug);
        CollectionStatistics.writeToDisk();
       }
       StreamHelper.deleteDir(Paths.get(Configuration.getRootDirectory(), "invertedIndex", "temp"));
    }


    /*private static class TokenPostingListPair{
        String token;
        PostingList postingList;

        public TokenPostingListPair(String token, PostingList postingList) {
            this.token = token;
            this.postingList = postingList;
        }

        int getSize(){
            return postingList.getSize();
        }
    }*/

}

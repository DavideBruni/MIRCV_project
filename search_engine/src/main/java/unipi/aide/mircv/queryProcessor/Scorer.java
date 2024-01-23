package unipi.aide.mircv.queryProcessor;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.model.*;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class Scorer {
    private static final double NORMALIZATION_PARAMETER_B = 0.70;
    private static final double NORMALIZATION_OPPOSITE_B = 0.30;
    private static final double NORMALIZATION_PARAMETER_K1 = 1.5;


    /**
     * Calculates the BM25 score for a single term in a document.
     *
     * @param tf     The term frequency (number of occurrences) of the term in the document.
     * @param docId  The identifier of the document.
     * @param df     The document frequency of the term.
     * @return The BM25 score for the term in the specified document.
     */
    public static double BM25_singleTermDocumentScore(int tf, int docId, double df) throws DocumentNotFoundException,ArithmeticException {
        int documentLength = DocumentIndex.getDocumentLength(docId);
        double idf = Math.log10(CollectionStatistics.getCollectionSize() / df);
        double averageDocumentLength = CollectionStatistics.getAverageDocumentLength();
        double Bj = NORMALIZATION_OPPOSITE_B + (NORMALIZATION_PARAMETER_B*(documentLength/averageDocumentLength));
        return (tf / ((NORMALIZATION_PARAMETER_K1*Bj) + tf)) * idf;
    }

    /**
     * Calculates the TF-IDF score for a single term in a document.
     *
     * @param tf The term frequency (number of occurrences) of the term in the document.
     * @param df The document frequency (number of documents containing the term) of the term.
     * @return The TF-IDF score for the term in the document.
     */
    public static double TFIDF_singleTermDocumentScore(int tf, double df) {
        return (1+ Math.log10(tf))*(Math.log10(CollectionStatistics.getCollectionSize() / df));
    }

    /**
     * Calculates the upper bounds of BM25 and TF-IDF scores for a term in a collection of documents.
     *
     * @param postingList The uncompressed posting list for the term.
     * @param idf The inverse document frequency (IDF) of the term.
     * @return An array containing the maximum BM25 score and the maximum TF-IDF score for the term in the collection.
     */
    public static double[] calculateTermUpperBounds(UncompressedPostingList postingList, double idf){
        double BM25_maxScore = 0.0;
        double TFIDF_maxScore = 0.0;
        double averageDocumentLength = CollectionStatistics.getAverageDocumentLength();
        List<Integer> docIds = postingList.getDocIds();
        List<Integer> frequencies = postingList.getFrequencies();
        for(int i = 0; i<docIds.size(); i++){
            int tf = frequencies.get(i);
            try {
                int documentLength = DocumentIndex.getDocumentLength(docIds.get(i));
                double Bj = NORMALIZATION_OPPOSITE_B + (NORMALIZATION_PARAMETER_B*(documentLength/averageDocumentLength));
                double BM25_score = (tf / ((NORMALIZATION_PARAMETER_K1*Bj) + tf)) * idf;
                double TFIDF_score = (1+Math.log10(tf))*idf;
                BM25_maxScore = Math.max(BM25_maxScore, BM25_score);
                TFIDF_maxScore = Math.max(TFIDF_maxScore, TFIDF_score);
            } catch (DocumentNotFoundException e) {
                CustomLogger.error("Document "+docIds.get(i)+ " not found");
            }
        }
        return new double[]{BM25_maxScore,TFIDF_maxScore};
    }


    /**
     * Computes the maximum scores for a set of posting lists using the MAX-SCORE algorithm.
     * The method returns a priority queue containing DocScorePair objects representing
     * the documents with the highest scores.
     * Array of PostingList must be provided sorted by term upper bounds in ascending order.
     *
     * @param postingLists          An array of posting lists sorted by upper bound in ascending order.
     * @param conjunctiveQuery      A boolean indicating whether the query is conjunctive or not.
     * @return                      A priority queue of (minHeapSize) DocScorePair objects sorted by descending scores.
     */
    public static PriorityQueue<DocScorePair> maxScore(PostingList[] postingLists, boolean conjunctiveQuery) {
        /* --------------- Initialize structures and doc upper bounds --------------- */
        PriorityQueue<DocScorePair> q = new PriorityQueue<>();
        int minHeapSize = Configuration.getMinheapDimension();
        double[] upperBounds = new double[postingLists.length];
        upperBounds[0] = postingLists[0].getTermUpperBound();

        for (int i = 1; i < postingLists.length; i++) {
            upperBounds[i] = upperBounds[i - 1] + postingLists[i].getTermUpperBound();      //necessary to understand if the posting list is essential or not
        }
        /* -------------------------------------------------------------------------- */
        double theta = 0;
        int pivot = 0;
        int idToSkip = -1;      //using only for conjuctive query
        int current = minimumDocid(postingLists);

        while (pivot < postingLists.length && current != Integer.MAX_VALUE) {
            double score = 0;
            int next = Integer.MAX_VALUE;
            /* --------------- Essential posting list  --------------- */
            for (int i = pivot; i <postingLists.length; i++) {
                if (postingLists[i].docId() == current) {
                    score += postingLists[i].score();
                    postingLists[i].next();
                /* ------------------------ CONJUCTIVE PART ------------------------*/
                } else if (conjunctiveQuery) {      // we have a postingList without the doc with minDocIdUsed
                    if(current != -1) {             // first document that doesn't have currentId
                        idToSkip = current;         // we don't have to consider this document anymore
                        current = -1;
                        score = 0;
                    }else{
                        //if the current posting has a docId = idToSkip --> skip it using next()
                        if(postingLists[i].docId() == idToSkip){
                            postingLists[i].next();
                        }
                    }
                }
                /* ----------------------------------------------------------------*/
                next = Math.min(next, postingLists[i].docId());
            }
            /* -------------------------------------------------------- */
            if(current > 0) {       //in conjuctive mode, I have to skip to next doc if current == -1
                /* --------------- Non-Essential posting list  --------------- */
                for (int i = pivot - 1; i >= 0; i--) {
                    if (score + upperBounds[i] <= theta) {
                        break;
                    }
                    postingLists[i].nextGEQ(current);
                    if (postingLists[i].docId() == current) {
                        score += postingLists[i].score();
                    } else if (conjunctiveQuery) {
                        break;
                    }
                }
                /* --------------- Add result to priority queue --------------- */
                if (q.add(new DocScorePair(current, score))) {
                    if (q.size() > minHeapSize) {       //if size more than minHeapSize, remove the one with the smallest score
                        q.poll();
                    }
                    if (q.size() == minHeapSize) {      //get theta
                        if (q.peek() != null) {
                            theta = q.peek().getScore();
                        }
                    }
                    /* --------------- Pivot updating --------------- */
                    while (pivot < postingLists.length && upperBounds[pivot] <= theta) {
                        pivot++;
                    }
                }
            }
            current = next;
        }

        /* ------- Reverse the order since I want to return the result in decreasing order of score --------- */
        PriorityQueue<DocScorePair> reversedQueue = new PriorityQueue<>(q.size(), Comparator.reverseOrder());
        reversedQueue.addAll(q);
        return reversedQueue;
    }

    /**
     * Utility function that finds and returns the minimum document ID among a set of posting lists.
     *
     * @param postingLists  An array of PostingList objects.
     * @return              The minimum document ID present in the given posting lists.
     *                      Returns Integer.MAX_VALUE if the posting lists are empty.
     */
    private static int minimumDocid(PostingList[] postingLists) {
        int minDocid = Integer.MAX_VALUE;
        for (PostingList postingList : postingLists) {
            int id = postingList.docId();
            minDocid = Math.min(id,minDocid);
        }
        return minDocid;
    }

    static class DocScorePair implements Comparable {
        private final int docid;
        private final double score;
        private String pid;

        public DocScorePair(int docid, double score) {
            this.docid = docid;
            this.score = score;
        }

        public double getScore() {
            return score;
        }

        public String getPid(){return DocumentIndex.getDocno(docid);}

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            DocScorePair docScorePair = (DocScorePair) obj;
            return docScorePair.docid == this.docid && docScorePair.score == this.score;
        }

        @Override
        public int compareTo(Object o) {
            DocScorePair tmp = (DocScorePair) o;
            return Double.compare(this.getScore(),tmp.getScore());
        }

        @Override
        public String toString() {
            if(pid == null){
                pid = DocumentIndex.getDocno(docid);
            }
            return  pid + "\t" + score;
        }


    }

}

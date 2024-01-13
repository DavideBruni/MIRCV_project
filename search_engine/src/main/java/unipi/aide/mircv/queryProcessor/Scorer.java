package unipi.aide.mircv.queryProcessor;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.model.*;

import java.io.Serializable;
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
     * @param idf    The inverse document frequency of the term.
     * @return The BM25 score for the term in the specified document.
     */
    public static double BM25_singleTermDocumentScore(int tf, int docId, double idf) throws DocumentNotFoundException,ArithmeticException {
        int documentLength = DocumentIndex.getDocumentLength(docId);
        double averageDocumentLength = CollectionStatistics.getDocumentsLen() / (double) CollectionStatistics.getCollectionSize();
        return (tf / (NORMALIZATION_PARAMETER_K1*((1-NORMALIZATION_PARAMETER_B) + (NORMALIZATION_PARAMETER_B*(documentLength/averageDocumentLength))) + tf)) * idf;
    }

    public static double TFIDF_singleTermDocumentScore(int tf, double idf) {
        return (1+ Math.log(tf))*idf;
    }

    public static double[] calculateTermUpperBounds(UncompressedPostingList postingList, double idf){
        double BM25_maxScore = 0.0;
        double TFIDF_maxScore = 0.0;
        double averageDocumentLength = CollectionStatistics.getDocumentsLen() / (double) CollectionStatistics.getCollectionSize();
        List<Integer> docIds = postingList.getDocIds();
        List<Integer> frequencies = postingList.getFrequencies();
        for(int i = 0; i<docIds.size(); i++){
            int tf = frequencies.get(i);
            try {
                int documentLength = DocumentIndex.getDocumentLength(docIds.get(i));
                double BM25_score = (tf / (NORMALIZATION_PARAMETER_K1*(NORMALIZATION_OPPOSITE_B + (NORMALIZATION_PARAMETER_B*(documentLength/averageDocumentLength))) + tf)) * idf;
                double TFIDF_score = (1+Math.log(tf))*idf;
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
     *
     * @param postingLists An array of posting lists sorted by upper bound in ascending order.
     * @param conjunctiveQuery A boolean indicating whether the query is conjunctive or not.
     * @return A priority queue of DocScorePair objects sorted by descending scores.
     *         The queue contains the documents with the highest scores, limited by the configured minHeapSize.
     */
    public static PriorityQueue<DocScorePair> maxScore(PostingList[] postingLists, boolean conjunctiveQuery) {
        PriorityQueue<DocScorePair> q = new PriorityQueue<>();
        int minHeapSize = Configuration.getMinheapDimension();
        double[] upperBounds = new double[postingLists.length];
        upperBounds[0] = postingLists[0].getTermUpperBound();

        for (int i = 1; i < postingLists.length; i++) {
            upperBounds[i] = upperBounds[i - 1] + postingLists[i].getTermUpperBound();      //necessary to understand if the posting list is essential or not
        }

        double theta = 0;
        int pivot = 0;
        int current = minimumDocid(postingLists);

        while (pivot < postingLists.length && current != Integer.MAX_VALUE-1) {
            double score = 0;
            int next = Integer.MAX_VALUE - 1;

            for (int i = pivot; i <postingLists.length; i++) { // Essential lists
                if (postingLists[i].docId() == current) {
                    score += postingLists[i].score();
                    try {
                        postingLists[i].next();
                    }catch (Exception e){
                        CustomLogger.error("Error calling next() function: "+e.getMessage());
                    }
                } else if (conjunctiveQuery) {      // we have a postingList without the doc with minDocIdUsed
                    current = -1;       // we don't have to consider this document anymore
                    score = 0;
                }
                if(conjunctiveQuery && current!=-1) {
                    try {
                        postingLists[i].next();
                    } catch (Exception e) {
                        CustomLogger.error("Error calling next() function: " + e.getMessage());
                    }
                }
                    if (postingLists[i].docId() < next) {       // if the current docId is lower than the candidate next, update the candidate next
                        next = postingLists[i].docId();
                    }
            }


            for (int i = pivot - 1; i >= 0; i--) { // Non-essential lists
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

            if (q.add(new DocScorePair(current, score))) { // List pivot update
                if (q.size() > minHeapSize) {
                    q.poll();
                }
                if (q.size() == minHeapSize) {
                    if (q.peek() != null) {
                        theta = q.peek().getScore();
                    }
                }

                while (pivot < postingLists.length && upperBounds[pivot] <= theta) {
                    pivot++;
                }
            }
            current = next;
        }

        PriorityQueue<DocScorePair> reversedQueue = new PriorityQueue<>(q.size(), Comparator.reverseOrder());
        reversedQueue.addAll(q);
        return reversedQueue;
    }

    private static int minimumDocid(PostingList[] postingLists) {
        int minDocid = Integer.MAX_VALUE;
        for (PostingList postingList : postingLists) {
            if (postingList.docId() < minDocid) {
                minDocid = postingList.docId();
            }
        }
        return minDocid;
    }

    static class DocScorePair implements Comparable, Serializable {
        private int docid;
        private double score;
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
            if(docScorePair.docid == this.docid && docScorePair.score == this.score)
                return true;
            return false;
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
            return  pid;
        }


    }

}

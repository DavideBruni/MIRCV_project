package unipi.aide.mircv.queryProcessor;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.model.PostingList;

import java.util.Comparator;
import java.util.PriorityQueue;

public class MaxScoreAlgorithm {

    public PriorityQueue<DocScorePair> maxScore(PostingList[] postingLists) {
        PriorityQueue<DocScorePair> q = new PriorityQueue<>(Comparator.comparingDouble(DocScorePair::getScore));
        int minHeapSize = Configuration.getMinheapDimension();
        double[] upperBounds = new double[postingLists.length];
        upperBounds[0] = postingLists[0].getTermUpperBound();

        for (int i = 1; i < postingLists.length; i++) {
            upperBounds[i] = upperBounds[i - 1] + postingLists[i].getTermUpperBound();
        }

        double theta = 0;
        int pivot = 0;
        long current = minimumDocid(postingLists);

        while (pivot < postingLists.length && current != Integer.MAX_VALUE) {
            double score = 0;
            long next = Long.MAX_VALUE;

            for (int i = pivot; i <postingLists.length; i++) { // Essential lists
                long minDocIdUsed = postingLists[i].docId();
                if (minDocIdUsed == current) {
                    score += postingLists[i].score();
                    postingLists[i].next();
                }

                if (postingLists[i].docId() < next) {
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
                }

                if (q.add(new DocScorePair(current, score))) { // List pivot update
                    if(q.size() > minHeapSize) {
                        q.poll();
                    }
                    if(q.size() == minHeapSize){
                        if (q.peek() != null) {
                            theta = q.peek().getScore();
                        }
                    }
                }
            }

            while (pivot < postingLists.length && upperBounds[pivot] <= theta) {
                pivot++;
            }
            current = next;
        }

        return q;
    }

    private static long minimumDocid(PostingList[] postingLists) {
        long minDocid = Integer.MAX_VALUE;
        for(int i = 0; i< postingLists.length; i++){
            if (postingLists[i].docId() < minDocid) {
                minDocid = postingLists[i].docId();
            }
        }
        return minDocid;
    }

    static class DocScorePair {
        private long docid;
        private double score;

        public DocScorePair(long docid, double score) {
            this.docid = docid;
            this.score = score;
        }

        public double getScore() {
            return score;
        }
    }
}

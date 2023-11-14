package unipi.aide.mircv.queryProcessor;

import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.model.*;

public class Scorer {
    private static final double NORMALIZATION_PARAMETER_B = 0.70;
    private static final double NORMALIZATION_PARAMETER_K1 = 1.5;


    public static double BM25_singleTermDocumentScore(int tf, long docId, double idf) throws DocumentNotFoundException,ArithmeticException {
        int documentLength = DocumentIndex.getDocumentLength(docId);
        double averageDocumentLength = CollectionStatistics.getDocumentsLen() / (double) CollectionStatistics.getCollectionSize();
        return (tf / (NORMALIZATION_PARAMETER_K1*((1-NORMALIZATION_PARAMETER_B) + (NORMALIZATION_PARAMETER_B*(documentLength/averageDocumentLength))) + tf)) * idf;
    }

    public static void BM25_termUpperBound(PostingList postingList, LexiconEntry lexiconEntry){
        double maxScore = 0.0;
        for(Posting posting : postingList.getPostingList()){
            int tf = posting.getFrequency();
            try {
                int documentLength = DocumentIndex.getDocumentLength(posting.getDocid());
                double averageDocumentLength = CollectionStatistics.getDocumentsLen() / (double) CollectionStatistics.getCollectionSize();
                double score = (tf / (NORMALIZATION_PARAMETER_K1*((1-NORMALIZATION_PARAMETER_B) + (NORMALIZATION_PARAMETER_B*(documentLength/averageDocumentLength))) + tf)) * lexiconEntry.getIdf();
                if (score > maxScore)
                    maxScore = score;
            } catch (DocumentNotFoundException e) {

            }
        }
        lexiconEntry.setTermUpperBound(maxScore);
    }
}

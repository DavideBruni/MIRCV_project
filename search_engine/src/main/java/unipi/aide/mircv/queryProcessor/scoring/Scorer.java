package unipi.aide.mircv.queryProcessor.scoring;

import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.model.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class Scorer {
    private static final double NORMALIZATION_PARAMETER_B = 0.70;
    private static final double NORMALIZATION_PARAMETER_K1 = 1.5;
    private static final int MIN_HEAP_SIZE = 10;


    public static double BM25_termUpperBound(List<Posting> postingList, LexiconEntry lexiconEntry){
        double maxScore = 0.0;
        for(Posting posting : postingList){
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
        return maxScore;
    }

    public static List<String> score(PostingLists postingLists, List<LexiconEntry> lexiconEntries, List<String> tokens){
        PriorityQueue<DocumentInfo> minHeap = new PriorityQueue<>(Comparator.comparingDouble(DocumentInfo::getScore));
        List<List<Posting>> essentialPostingLists = new ArrayList<>();
        List<List<Posting>> nonEssentialPostingLists = new ArrayList<>();
        int currentTreshold = Integer.MAX_VALUE;
        for(int i = 0; i<MIN_HEAP_SIZE;i++){
            // recupero il docId minore tra le postingList
            // segno le postingList da usare
            // next() + calcolo lo score del documento
            // aggiungo lo score al documentInfo
            // aggiungo il documentInfo al minHeap
            // mantengo la treshold
        }

        //divido in essential e non essential
        for(LexiconEntry lexiconEntry : lexiconEntries){
            List<Posting> postingList = postingLists.getPostingList(tokens.get(lexiconEntries.indexOf(lexiconEntry)));
            if (lexiconEntry.getTermUpperBound() > currentTreshold){
                essentialPostingLists.add(postingList);
            }else{
                nonEssentialPostingLists.add(postingList);
            }
        }

        // analizzo le essential e calcolo il partialScore per il documento con id minore

        // se partialScore + nonEssentialTermUpperBounds > treshold
        //  THEN aggiorna partialScore e documenentUpperbound in base alle non essentialList (prima quelle con score più alto)
        //  SE minore --> skip, SE maggiore aggiorna la treshold, il minHeap e lo split in essential - non essential

        // STRUTTURE DATI DA MANTENERE:
        // - POSTING LIST ORDINATE PER TERM UPPERBOUND
        // ALTRO
        return null;
    }
}

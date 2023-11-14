package unipi.aide.mircv.model;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.DocIdNotFoundException;
import unipi.aide.mircv.exceptions.DocumentNotFoundException;
import unipi.aide.mircv.queryProcessor.Scorer;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PostingList {
    List<Posting> postingList;
    // score
    String term;
    int currentDocIdIndex;
    boolean inMemory;
    int numBlockRead;

    public PostingList(List<Posting> postingList,String term) {
        this.term = term;
        this.postingList = postingList;
        currentDocIdIndex = -1;
        inMemory = true;
    }

    public PostingList() {
        postingList = new ArrayList<>();
        currentDocIdIndex = -1;
        inMemory = true;
    }

    public PostingList(String term){
        postingList = new ArrayList<>();
        currentDocIdIndex = -1;
        inMemory = true;
        this.term = term;

    }


    public long docId() {
        if(currentDocIdIndex == -1){        //never used this postingList
            next();
        }
        if(!inMemory){                      // se non è in memoria significa che ho letto tutte le partizioni
            return Long.MAX_VALUE;
        }
        return postingList.get(currentDocIdIndex).docid;
    }

    public void add(Posting posting) {
        postingList.add(posting);
    }

    public List<Posting> getPostingList() {
        return postingList;
    }

    public double score(){
        try {
            return Scorer.BM25_singleTermDocumentScore(postingList.get(currentDocIdIndex).frequency,postingList.get(currentDocIdIndex).docid, Lexicon.getEntryValue(term,LexiconEntry::getIdf) );
        } catch (DocumentNotFoundException | ArithmeticException e ) {
            return 0;
        }
    }

    public void next(){
        // cases
        /*
         *  non è in memoria:
         *      se non ho letto tutti i bloochi
         *           la leggo e recupero il primo id
         *       altrimenti
         *           restituisco Long.MAX_VALUE
         *  è in memoria
         *      provo a recuperare l'id:
         *           se indexOutOfBoundException:
         *               se non ho letto tutti i bloochi
         *                  la leggo e recupero il primo id
         *              altrimenti
         *                  restituisco Long.MAX_VALUE
         *                  inMemory = false
         *
         *
         * */


        if((!inMemory && Lexicon.getEntryValue(term,LexiconEntry::getNumBlocks)  != numBlockRead) ||
                (inMemory && ++currentDocIdIndex > postingList.size() && Lexicon.getEntryValue(term,LexiconEntry::getNumBlocks)  != numBlockRead)) {
            SkipPointer skipPointer;
            try {
                skipPointer = SkipPointer.readFromDisk(Lexicon.getEntryValue(term,LexiconEntry::getSkipPointerOffset) ,numBlockRead);
            } catch (IOException e) {
                // handle it
                throw new RuntimeException(e);
            }
            numBlockRead++;
            int docIdOffset = skipPointer.getDocIdsOffset();
            int frequencyOffset = skipPointer.getFrequencyOffset();
            postingList = readFromDisk(docIdOffset,frequencyOffset);
            currentDocIdIndex = 0;
        }else{
            inMemory = false;
        }
    }

    private List<Posting> readFromDisk(int docIdOffset, int frequencyOffset) {
        List<Posting> res = new ArrayList<>();
        try (FileInputStream docStream = new FileInputStream(Configuration.DOCUMENT_IDS_PATH);
             FileInputStream freqStream = new FileInputStream(Configuration.FREQUENCY_PATH)){
            if (!Configuration.isCOMPRESSED()) {
                DataInputStream dis_docStream = new DataInputStream(docStream);
                DataInputStream dis_freqStream = new DataInputStream(freqStream);
                dis_docStream.skipBytes(docIdOffset);
                dis_freqStream.skipBytes(frequencyOffset);
                for (int i = 0; i < Lexicon.getEntryValue(term,LexiconEntry::getPostingNumber); i++) {
                    try {
                        long docId = dis_docStream.readLong();
                        int frq = dis_freqStream.readInt();
                        res.add(new Posting(docId,frq));
                    } catch (EOFException eof) {
                        break;
                    }
                }
                dis_docStream.close();
                dis_freqStream.close();
            }else{
                List<Long> docIds = EliasFano.decompress(docStream, docIdOffset);
                List<Integer> frequency = UnaryCompressor.readFrequencies(freqStream, frequencyOffset,Lexicon.getEntryValue(term,LexiconEntry::getPostingNumber));
                for(int i = 0; i<docIds.size(); i++){
                    res.add(new Posting(docIds.get(i),frequency.get(i)));
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    public void nextGEQ(long docId){

        // cases
        /*
         *  non è in memoria:
         *      se non ho letto tutti i bloochi
         *           la leggo e recupero l'id se presente e aggiorno la posizione di questo id
         *       altrimenti
         *           return
         *  è in memoria
         *      provo a recuperare l'id:
         *           se indexOutOfBoundException:
         *               se non ho letto tutti i bloochi
         *                  la leggo e l'id se presente e aggiorno la posizione di questo id
         *              altrimenti
         *                  return
         *                  inMemory = false
         *
         *
         * */
        int numBlocks = Lexicon.getEntryValue(term,LexiconEntry::getNumBlocks);
        if((!inMemory && numBlocks != numBlockRead) ||
                (inMemory && ++currentDocIdIndex > postingList.size() && numBlocks != numBlockRead)) {
            try {
                SkipPointer skipPointer;
                while(true) {
                    skipPointer = SkipPointer.readFromDisk(numBlocks, numBlockRead);
                    numBlockRead++;
                    if(skipPointer.getMaxDocId() >= docId)
                       break;
                    if (numBlocks == numBlockRead)
                        throw new DocIdNotFoundException();
                }
                int docIdOffset = skipPointer.getDocIdsOffset();
                int frequencyOffset = skipPointer.getFrequencyOffset();
                postingList = readFromDisk(docIdOffset, frequencyOffset);
                for(int i = 0; i< postingList.size(); i++){
                    if(docId == postingList.get(i).docid){
                        currentDocIdIndex = i;
                        return;
                    }
                }
            }catch(DocIdNotFoundException d) {
                inMemory = false;
            } catch (IOException e) {
                inMemory = false;
            }
        }else{
            inMemory = false;
        }
    }

    public double getTermUpperBound() {
        return Lexicon.getEntryValue(term,LexiconEntry::getTermUpperBound);
    }
}

package unipi.aide.mircv.queryProcessor;

import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.MissingCollectionStatisticException;
import unipi.aide.mircv.model.CollectionStatistics;
import unipi.aide.mircv.model.Lexicon;
import unipi.aide.mircv.model.PostingList;
import unipi.aide.mircv.parsing.Parser;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import static junit.framework.TestCase.assertEquals;

public class ScorerTest {

    @BeforeEach
    void setUp() {
        // Reset the state before each test
        CollectionStatistics.reset(); // Add a method to reset or reinitialize the state
    }
    @Test
    public void maxScoreDisjunctiveQueryPostingListWithOneBlockLessResultThanTheMinHeapSizeTest() throws MissingCollectionStatisticException {
        Configuration.setUpPaths("toyScraper/data");
        Configuration.setMinheapDimension(10);
        Lexicon lexicon = Lexicon.getInstance();
        CollectionStatistics.readFromDisk();

        List<String> parsedQuery = Parser.getTokens("cat chair",true);

        PostingList[] postingLists = getPostingLists(parsedQuery);

        Comparator<PostingList> termUpperBoundComparator = Comparator.comparingDouble(postingList ->
                Lexicon.getEntry(postingList.getToken()).getTermUpperBound());
        Arrays.sort(postingLists, termUpperBoundComparator);
        PriorityQueue<Scorer.DocScorePair> result = Scorer.maxScore(postingLists, false);

        PriorityQueue<Scorer.DocScorePair> expectedResult = new PriorityQueue<>(Comparator.reverseOrder());
        expectedResult.add(new Scorer.DocScorePair(1,0.2599180942069478));
        expectedResult.add(new Scorer.DocScorePair(4,0.19325207920424972));
        expectedResult.add(new Scorer.DocScorePair(2,0.1299590471034739));
        expectedResult.add(new Scorer.DocScorePair(3,0.11084072844026584));

        while (!expectedResult.isEmpty()) {
            assertEquals(expectedResult.poll(),result.poll());
        }

    }

    @Test
    public void maxScoreDisjunctiveQueryPostingListWithOneBlockMoreResultThanTheMinHeapSizeTest() throws MissingCollectionStatisticException {
        Configuration.setUpPaths("toyScraper/data");
        Configuration.setMinheapDimension(2);
        Lexicon lexicon = Lexicon.getInstance();
        CollectionStatistics.readFromDisk();

        List<String> parsedQuery = Parser.getTokens("cat chair",true);

        PostingList[] postingLists = getPostingLists(parsedQuery);


        Comparator<PostingList> termUpperBoundComparator = Comparator.comparingDouble(postingList ->
                Lexicon.getEntry(postingList.getToken()).getTermUpperBound());
        Arrays.sort(postingLists, termUpperBoundComparator);
        PriorityQueue<Scorer.DocScorePair> result = Scorer.maxScore(postingLists, false);

        PriorityQueue<Scorer.DocScorePair> expectedResult = new PriorityQueue<>(Comparator.reverseOrder());
        expectedResult.add(new Scorer.DocScorePair(1,0.2599180942069478));
        expectedResult.add(new Scorer.DocScorePair(4,0.19325207920424972));

        while (!expectedResult.isEmpty()) {
            assertEquals(expectedResult.poll(),result.poll());
        }

    }

    @Test
    public void maxScoreConjuctiveQueryPostingListWithOneBlockLessResultThanTheMinHeapSizeTest() throws MissingCollectionStatisticException {
        Configuration.setUpPaths("toyScraper/data");
        Configuration.setMinheapDimension(10);
        Lexicon lexicon = Lexicon.getInstance();
        CollectionStatistics.readFromDisk();

        List<String> parsedQuery = Parser.getTokens("cat chair",true);

        PostingList[] postingLists = getPostingLists(parsedQuery);

        Comparator<PostingList> termUpperBoundComparator = Comparator.comparingDouble(postingList ->
                Lexicon.getEntry(postingList.getToken()).getTermUpperBound());
        Arrays.sort(postingLists, termUpperBoundComparator);
        PriorityQueue<Scorer.DocScorePair> result = Scorer.maxScore(postingLists, true);

        PriorityQueue<Scorer.DocScorePair> expectedResult = new PriorityQueue<>(Comparator.reverseOrder());
        expectedResult.add(new Scorer.DocScorePair(1,0.2599180942069478));
        expectedResult.add(new Scorer.DocScorePair(4,0.19325207920424972));

        while (!expectedResult.isEmpty()) {
            assertEquals(expectedResult.poll(),result.poll());
        }

    }


    @Test
    public void maxScoreConjuctiveQueryPostingListWithOneBlockMoreResultsThanTheMinHeapSizeTest() throws MissingCollectionStatisticException {
        Configuration.setUpPaths("toyScraper/data");
        Configuration.setMinheapDimension(1);
        Lexicon lexicon = Lexicon.getInstance();
        CollectionStatistics.readFromDisk();

        List<String> parsedQuery = Parser.getTokens("cat chair",true);

        PostingList[] postingLists = getPostingLists(parsedQuery);

        Comparator<PostingList> termUpperBoundComparator = Comparator.comparingDouble(postingList ->
                Lexicon.getEntry(postingList.getToken()).getTermUpperBound());
        Arrays.sort(postingLists, termUpperBoundComparator);
        PriorityQueue<Scorer.DocScorePair> result = Scorer.maxScore(postingLists, true);

        PriorityQueue<Scorer.DocScorePair> expectedResult = new PriorityQueue<>(Comparator.reverseOrder());
        expectedResult.add(new Scorer.DocScorePair(1,0.2599180942069478));

        while (!expectedResult.isEmpty()) {
            assertEquals(expectedResult.poll(),result.poll());
        }

    }


    @Test
    public void maxScoreDisjunctiveQueryPostingListWithMoreThanOneBlockTest() throws MissingCollectionStatisticException {
        Configuration.setUpPaths("data");
        Configuration.setMinheapDimension(5);
        Lexicon lexicon = Lexicon.getInstance();
        CollectionStatistics.readFromDisk();

        List<String> parsedQuery = Parser.getTokens("02 09 explosive",true);

        PostingList[] postingLists = getPostingLists(parsedQuery);

        Comparator<PostingList> termUpperBoundComparator = Comparator.comparingDouble(postingList ->
                Lexicon.getEntry(postingList.getToken()).getTermUpperBound());
        Arrays.sort(postingLists, termUpperBoundComparator);
        PriorityQueue<Scorer.DocScorePair> result = Scorer.maxScore(postingLists, false);

        PriorityQueue<Scorer.DocScorePair> expectedResult = new PriorityQueue<>(Comparator.reverseOrder());
        expectedResult.add(new Scorer.DocScorePair(604,3.237364904641662));
        expectedResult.add(new Scorer.DocScorePair(602,2.972750497194531));
        expectedResult.add(new Scorer.DocScorePair(2065,2.876099758485594));
        expectedResult.add(new Scorer.DocScorePair(2070,2.823007137425152));
        expectedResult.add(new Scorer.DocScorePair(2511,2.499963014797879));


        while (!expectedResult.isEmpty()) {
            assertEquals(expectedResult.poll(),result.poll());
        }

    }


    private static PostingList[] getPostingLists(List<String> terms) {
        PostingList [] postingLists = new PostingList[terms.size()];
        for(String term : terms){
            postingLists[terms.indexOf(term)] = new PostingList(term);
        }
        return postingLists;
    }
}

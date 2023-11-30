package unipi.aide.mircv.queryProcessor;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.MissingCollectionStatisticException;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.model.*;
import unipi.aide.mircv.parsing.Parser;


import java.util.*;

public class QueryProcessorMain {
    private static final int RESULTS_CACHE_CAPACITY = 1000;

    public static void main( String[] args ){
        if (args.length < 3 ) {
            System.err.println("Error in input parameters, parameters are:");
            System.err.println("< parse document >       Boolean");
            System.err.println("< compress index >       Boolean");
            System.err.println("< k >                    Integer, how many results show");
            System.exit(-1);
        }

        boolean parse = Boolean.parseBoolean(args[0]);
        Configuration.setCOMPRESSED(Boolean.parseBoolean(args[1]));

        Configuration.setMinheapDimension(Integer.parseInt(args[2]));
        Scanner scanner = new Scanner(System.in);

        Configuration.setUpPaths("data");
        // TODO read cache
        LinkedHashMap<String,PriorityQueue<Scorer.DocScorePair>> searchResultCache = new LinkedHashMap<>(RESULTS_CACHE_CAPACITY, 0.8f, true) {
            protected boolean removeEldestEntry(Map.Entry<String, PriorityQueue<Scorer.DocScorePair>> eldest)
            {
                return size() > RESULTS_CACHE_CAPACITY;
            }
        };


        try {
            CollectionStatistics.readFromDisk();
        } catch (MissingCollectionStatisticException e) {
            CustomLogger.error("Error in setting up environment");
        }

        Lexicon lexicon = Lexicon.getInstance();

        System.out.println("To perform conjuctive query, start it with \"+\" character\n");

        while(true){
            boolean is_cunjuctive = false;
            boolean add_result_to_cache = true;

            System.out.println("Insert new query\n");
            // Read query from stdin
            String query = scanner.nextLine();
            //exit condition    CTRL+E
            if(query.isEmpty()){
                continue;
            }
            if(query.equals("q"))
                break;
            if(query.trim().charAt(0) == '+' )
                is_cunjuctive=true;
            long timestamp_start = System.currentTimeMillis();
            List<String> parsedQuery = Parser.getTokens(query,parse);

            PriorityQueue<Scorer.DocScorePair> queryResult = null;

            queryResult = searchResultCache.get(String.join(" ",parsedQuery));
            if(queryResult == null) {
                PostingList[] postingLists = getPostingLists(parsedQuery);
                if (postingLists.length != 0) {
                    // posting list must be sorted based on term upperBound
                    Comparator<PostingList> termUpperBoundComparator = Comparator.comparingDouble(postingList ->
                            Lexicon.getEntry(postingList.getToken(),true).getTermUpperBound());
                    Arrays.sort(postingLists, termUpperBoundComparator);
                    queryResult = Scorer.maxScore(postingLists, is_cunjuctive);
                }
            }else
                add_result_to_cache = false;

            // printing result
            if(queryResult == null){
                System.out.println("No results found! \n\n");
            }else{
                System.out.println(queryResult + "\n\n");
            }
            long timestamp_stop = System.currentTimeMillis();
            System.out.println("("+(timestamp_stop-timestamp_start)+" milliseconds )");
            if(add_result_to_cache)
                searchResultCache.put(String.join(" ",parsedQuery),queryResult);

        }
        scanner.close();
        // TODO store cache

    }

    private static PostingList[] getPostingLists(List<String> terms) {
        List<PostingList> postingLists = new ArrayList<>();
        for(String term : terms){
            PostingList postingList = new PostingList(term,false);
            if(postingList.docId()!=Integer.MAX_VALUE)
                postingLists.add(postingList);
        }
        return postingLists.toArray(new PostingList[0]);
    }
}

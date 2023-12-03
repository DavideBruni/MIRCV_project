package unipi.aide.mircv.queryProcessor;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.MissingCollectionStatisticException;
import unipi.aide.mircv.helpers.StreamHelper;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.model.*;
import unipi.aide.mircv.parsing.Parser;


import java.io.*;
import java.util.*;

public class QueryProcessorMain {
    private static final int RESULTS_CACHE_CAPACITY = 1000;
    private static final String SEARCH_RESULT_CACHE_FILENAME = "searchResult.dat";
    public static void main( String[] args ){
        if (args.length < 4 ) {
            System.err.println("Error in input parameters, parameters are:");
            System.err.println("< parse document >       Boolean");
            System.err.println("< compress index >       Boolean");
            System.err.println("< is TREC eval   >       Boolean");
            System.err.println("< k >                    Integer, how many results show");
            System.exit(-1);
        }

        boolean parse = Boolean.parseBoolean(args[0]);
        boolean trec = Boolean.parseBoolean(args[2]);


        Configuration.setUpPaths("data");
        try {
            CollectionStatistics.readFromDisk();
        } catch (MissingCollectionStatisticException e) {
            CustomLogger.error("Error in setting up environment");
            System.exit(-3);
        }
        Configuration.setCOMPRESSED(Boolean.parseBoolean(args[1]));
        Configuration.setMinheapDimension(Integer.parseInt(args[3]));
        Lexicon lexicon = Lexicon.getInstance();
        if(trec){
            evaluation();
        }else {
            LinkedHashMap<String,PriorityQueue<Scorer.DocScorePair>> searchResultCache = new LinkedHashMap<>(RESULTS_CACHE_CAPACITY, 0.8f, true) {
                protected boolean removeEldestEntry(Map.Entry<String, PriorityQueue<Scorer.DocScorePair>> eldest) {
                    return size() > RESULTS_CACHE_CAPACITY;
                }
            };

            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(Configuration.getRootDirectory()+"/cache/"+SEARCH_RESULT_CACHE_FILENAME))) {
                searchResultCache.putAll((LinkedHashMap<String, PriorityQueue<Scorer.DocScorePair>>) ois.readObject());
            } catch (IOException | ClassNotFoundException e) {
                CustomLogger.info("Previous cache not found");
            }

            System.out.println("To perform conjuctive query, start it with \"+\" character\n");

            Scanner scanner = new Scanner(System.in);
            while (true) {
                boolean is_cunjuctive = false;
                boolean add_result_to_cache = true;

                System.out.println("Insert new query\n");
                // Read query from stdin
                String query = scanner.nextLine();

                if (query.isEmpty()) {
                    continue;
                }
                //exit condition
                if (query.equals("q"))
                    break;
                if (query.trim().charAt(0) == '+')
                    is_cunjuctive = true;
                long timestamp_start = System.currentTimeMillis();
                List<String> parsedQuery = Parser.getTokens(query, parse);
                Collections.sort(parsedQuery);

                PriorityQueue<Scorer.DocScorePair> queryResult = null;

                queryResult = searchResultCache.get(String.join(" ", parsedQuery));
                if (queryResult == null) {
                    PostingList[] postingLists = getPostingLists(parsedQuery);
                    if (postingLists.length != 0) {
                        // posting list must be sorted based on term upperBound
                        Comparator<PostingList> termUpperBoundComparator = Comparator.comparingDouble(postingList ->
                                Lexicon.getEntry(postingList.getToken(), true).getTermUpperBound());
                        Arrays.sort(postingLists, termUpperBoundComparator);
                        queryResult = Scorer.maxScore(postingLists, is_cunjuctive);
                    }
                } else
                    add_result_to_cache = false;

                long timestamp_stop = System.currentTimeMillis();
                // printing result
                if (queryResult == null) {
                    System.out.println("No results found! \n\n");
                } else {
                    System.out.println(queryResult + "\n\n");
                }

                System.out.println("(" + (timestamp_stop - timestamp_start) + " milliseconds )");
                if (add_result_to_cache)
                    searchResultCache.put(String.join(" ", parsedQuery), queryResult);

            }
            scanner.close();

            Configuration.setUpPaths("data");
            //store cache
            StreamHelper.createDir(Configuration.getRootDirectory()+"/cache");
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(SEARCH_RESULT_CACHE_FILENAME))) {
                oos.writeObject(searchResultCache);
                System.out.println("Dati scritti su file con successo.");
            } catch (IOException e) {
                CustomLogger.error("Not able to store search result cache");
            }

        }

    }

    private static void evaluation() {

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


    private static PriorityQueue<Scorer.DocScorePair> queryHandler(List<String> parsedQuery,boolean is_cunjuctive){
        PriorityQueue<Scorer.DocScorePair> queryResult= new PriorityQueue<>();
        PostingList[] postingLists = getPostingLists(parsedQuery);
        if (postingLists.length != 0) {
            // posting list must be sorted based on term upperBound
            Comparator<PostingList> termUpperBoundComparator = Comparator.comparingDouble(postingList ->
                    Lexicon.getEntry(postingList.getToken(),true).getTermUpperBound());
            Arrays.sort(postingLists, termUpperBoundComparator);
            queryResult = Scorer.maxScore(postingLists, is_cunjuctive);
        }
        return queryResult;
    }
}

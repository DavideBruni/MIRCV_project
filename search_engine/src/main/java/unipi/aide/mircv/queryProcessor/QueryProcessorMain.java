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
    private static final int RESULTS_CACHE_CAPACITY = 100;
    private static final String SEARCH_RESULT_CACHE_FILENAME = "searchResult.dat";
    public static void main( String[] args ){
        if (args.length < 5 ) {
            System.err.println("Error in input parameters, parameters are:");
            System.err.println("< parse document >                              Boolean");
            System.err.println("< compress index >                              Boolean");
            System.err.println("< score standard BM25 or default (TFIDF) >      String");
            System.err.println("< is TREC eval   >                              Boolean");
            System.err.println("< k, how many results show >                    Integer");
            System.exit(-1);
        }

        boolean parse = Boolean.parseBoolean(args[0]);
        boolean trec = Boolean.parseBoolean(args[3]);

        Configuration.setUpPaths("data");
        Lexicon lexicon = Lexicon.getInstance();
        Configuration.setCOMPRESSED(Boolean.parseBoolean(args[1]));
        try {
            CollectionStatistics.readFromDisk();
        } catch (MissingCollectionStatisticException e) {
            CustomLogger.error("Error in setting up environment");
            System.exit(-3);
        }
        Configuration.setMinheapDimension(Integer.parseInt(args[4]));
        Configuration.setScoreStandard(args[2]);

        if(trec){
            evaluation(parse);
        }else {

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
                //Collections.sort(parsedQuery);
                PriorityQueue<Scorer.DocScorePair> queryResult = queryHandler(parsedQuery,is_cunjuctive);

                long timestamp_stop = System.currentTimeMillis();
                // printing result
                if (queryResult == null) {
                    System.out.println("No results found! \n\n");
                } else {
                    for(Scorer.DocScorePair tmp : queryResult)
                        System.out.println(tmp);
                }
                System.out.println("(" + (timestamp_stop - timestamp_start) + " milliseconds )");

            }
            scanner.close();
        }

    }

    private static void evaluation(boolean parse) {
        List<String> results = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader("msmarco-test2020-queries.tsv"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String id = parts[0];
                    String query = parts[1];
                    List<String> parsedQuery = Parser.getTokens(query, parse);
                    Collections.sort(parsedQuery);
                    PriorityQueue<Scorer.DocScorePair> queryResult = queryHandler(parsedQuery,false);
                    int i = 1;
                    for(Scorer.DocScorePair result : queryResult){
                        String resultLine = id + " Q0 " + result.getPid() + " "
                                + i++ + " " + result.getScore() + " BM25";
                        results.add(resultLine);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(Configuration.getCachePath()))) {
            for (String resultLine : results) {
                bw.write(resultLine);
                bw.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    private static PostingList[] getPostingLists(List<String> terms) {
        List<PostingList> postingLists = new ArrayList<>();
        for(String term : terms){
            PostingList postingList = null;
            try {
                postingList = PostingList.loadFromDisk(term);
            } catch (IOException e) {
                continue;               // aggiungi stampa
            }
            if(postingList != null)
                postingLists.add(postingList);

        }
        return postingLists.toArray(new PostingList[0]);
    }


    private static PriorityQueue<Scorer.DocScorePair> queryHandler(List<String> parsedQuery,boolean is_cunjuctive){
        PriorityQueue<Scorer.DocScorePair> queryResult= new PriorityQueue<>();
        PostingList[] postingLists = getPostingLists(parsedQuery);
        if (postingLists.length != 0) {
            // posting list must be sorted based on term upperBound
            Comparator<PostingList> termUpperBoundComparator = Comparator.comparing(PostingList::getTermUpperBound);
            Arrays.sort(postingLists, termUpperBoundComparator);
            queryResult = Scorer.maxScore(postingLists, is_cunjuctive);
        }
        return queryResult;
    }
}

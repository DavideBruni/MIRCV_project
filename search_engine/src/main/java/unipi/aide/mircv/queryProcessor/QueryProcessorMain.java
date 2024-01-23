package unipi.aide.mircv.queryProcessor;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.MissingCollectionStatisticException;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.model.*;
import unipi.aide.mircv.parsing.Parser;


import java.io.*;
import java.util.*;

public class QueryProcessorMain {
    public static void main( String[] args ){
        /*------------------------- INITIALIZATION OF USEFUL STRUCTURE ----------------------------*/
        /* Getting input parameters */
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
        /* End of handle input parameters */
        Configuration.setUpPaths("data");                       //set up root directory
        Lexicon lexicon = Lexicon.getInstance();                //instantiate an empty vocabulary
        Configuration.setCOMPRESSED(Boolean.parseBoolean(args[1]));
        try {
            CollectionStatistics.readFromDisk();                //recover collection statistics (collection lengths, docLen)
        } catch (MissingCollectionStatisticException e) {
            CustomLogger.error("Error in setting up environment");
            System.exit(-3);
        }
        Configuration.setMinheapDimension(Integer.parseInt(args[4]));
        Configuration.setScoreStandard(args[2]);
        if (Configuration.getScoreStandard().equals("BM25")){       //if BM25, load the DocumentIndex from disk
            DocumentIndex.loadFromDisk();
        }
        /*------------------------- END OF INITIALIZATION OF USEFUL STRUCTURE ----------------------------*/
        if(trec){
            evaluation(parse);
        }else {
            /*------------------------- GUI ----------------------------*/
            System.out.println("To perform conjuctive query, start it with \"+\" character\n");
            Scanner scanner = new Scanner(System.in);
            while (true) {
                boolean is_cunjuctive = false;
                System.out.println("Insert new query\n");
                String query = scanner.nextLine();      // Read query from stdin
                if (query.isEmpty()) {
                    continue;
                }
                if (query.equals("q"))                  //exit condition
                    break;
                if (query.trim().charAt(0) == '+')      //conjuctive condition
                    is_cunjuctive = true;
                long timestamp_start = System.currentTimeMillis();
                List<String> parsedQuery = null;
                try {
                    parsedQuery = Parser.getTokens(query, parse);
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
                /* ------------------ MOST IMPORTANT PART OF QUERY PROCESSING ------------------*/
                PriorityQueue<Scorer.DocScorePair> queryResult = queryHandler(parsedQuery,is_cunjuctive);

                long timestamp_stop = System.currentTimeMillis();
                // printing result
                if (queryResult == null) {
                    System.out.println("No results found! \n\n");
                } else {
                    while (!queryResult.isEmpty()) {
                        System.out.println(queryResult.poll());
                    }
                }
                System.out.println("(" + (timestamp_stop - timestamp_start) + " milliseconds )");

            }
            scanner.close();
        }
        /*------------------------- END GUI ----------------------------*/

    }

    private static void evaluation(boolean parse) {
        List<String> results = new ArrayList<>();
        List<Long> times = new ArrayList<>();
        String filename = "qres_"+Configuration.getScoreStandard()+"_";
        if (parse)
            filename += "parsed.txt";
        else
            filename += "not_parsed.txt";
        try (BufferedReader br = new BufferedReader(new FileReader("msmarco-test2020-queries.tsv"));
             BufferedWriter resultStream = new BufferedWriter(new FileWriter(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String id = parts[0];
                    String query = parts[1];
                    List<String> parsedQuery = Parser.getTokens(query, parse);
                    long start = System.currentTimeMillis();
                    Collections.sort(parsedQuery);
                    PriorityQueue<Scorer.DocScorePair> queryResult = queryHandler(parsedQuery,false);
                    long stop = System.currentTimeMillis();
                    times.add(stop - start);
                    int i = 1;
                    while (!queryResult.isEmpty()) {
                        Scorer.DocScorePair result = queryResult.poll();
                        String resultLine = id + " Q0 " + result.getPid() + " "
                                + i++ + " " + result.getScore() + " "+Configuration.getScoreStandard();
                        resultStream.write(resultLine);
                        resultStream.newLine();
                    }
                }
            }
            long sum = 0;
            for (Long value : times) {
                sum += value;
            }
            System.out.println("Avg time = " + sum/ times.size() +" ms");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    /**
     * Retrieves posting lists from disk for a given list of terms.
     *
     * @param       terms A list of strings representing the parsed query
     * @return      An array of PostingList objects containing the posting lists
     */
    private static PostingList[] getPostingLists(List<String> terms) {
        List<PostingList> postingLists = new ArrayList<>();
        for(String term : terms){
            PostingList postingList = null;
            try {
                postingList = PostingList.loadFromDisk(term);
            } catch (IOException e) {
                System.out.println(e.getMessage());
                continue;               // aggiungi stampa
            }
            if(postingList != null)
                postingLists.add(postingList);

        }
        return postingLists.toArray(new PostingList[0]);
    }

    /**
     * Handles the processing of a query using MaxScore as dynamic pruning algorithm.
     * This function retrieves posting lists for parsed query terms, sorts them based on term upper bounds
     * and applies MaxScore.
     *
     * @param parsedQuery   A list of strings representing the parsed query terms.
     * @param is_conjunctive A boolean flag indicating whether the query is conjunctive or disjunctive.
     * @return A PriorityQueue of DocScorePair objects representing the document scores for the given query.
     */
    private static PriorityQueue<Scorer.DocScorePair> queryHandler(List<String> parsedQuery,boolean is_conjunctive){
        PriorityQueue<Scorer.DocScorePair> queryResult= new PriorityQueue<>();
        PostingList[] postingLists = getPostingLists(parsedQuery);
        if (postingLists.length != 0) {
            // posting list must be sorted based on term upperBound
            Comparator<PostingList> termUpperBoundComparator = Comparator.comparing(PostingList::getTermUpperBound);
            Arrays.sort(postingLists, termUpperBoundComparator);
            queryResult = Scorer.maxScore(postingLists, is_conjunctive);
        }
        return queryResult;
    }
}

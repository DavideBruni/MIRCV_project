package unipi.aide.mircv.queryProcessor;

import unipi.aide.mircv.configuration.Configuration;
import unipi.aide.mircv.exceptions.MissingCollectionStatisticException;
import unipi.aide.mircv.log.CustomLogger;
import unipi.aide.mircv.model.CollectionStatistics;
import unipi.aide.mircv.model.Lexicon;
import unipi.aide.mircv.model.PostingList;
import unipi.aide.mircv.parsing.Parser;

import java.util.*;

public class QueryProcessorMain {
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

        try {
            CollectionStatistics.readFromDisk();
        } catch (MissingCollectionStatisticException e) {
            CustomLogger.error("Error in setting up environment");
        }

        Lexicon lexicon = Lexicon.getInstance();

        System.out.println("To perform conjuctive query, start it with \"+\" character\n");

        while(true){
            boolean is_cunjuctive = false;
            System.out.println("Insert new query\n");
            // Read query from stdin
            String query = scanner.nextLine();
            //exit condition    CTRL+E
            if(query.equals("\u0005"))
                break;
            if( query.trim().charAt(0) == '+' )
                is_cunjuctive=true;
            long timestamp_start = System.currentTimeMillis();
            List<String> parsedQuery = Parser.getTokens(query,parse);

            PostingList[] postingLists = getPostingLists(parsedQuery);
            PriorityQueue<Scorer.DocScorePair> queryResult = null;
            if (postingLists.length != 0) {
                // posting list must be sorted based on term upperBound
                Comparator<PostingList> termUpperBoundComparator = Comparator.comparingDouble(postingList ->
                        Lexicon.getEntry(postingList.getToken()).getTermUpperBound());
                Arrays.sort(postingLists, termUpperBoundComparator);
                queryResult = Scorer.maxScore(postingLists, is_cunjuctive);
            }

            // printing result
            if(queryResult == null){
                System.out.println("No results found! \n\n");
            }else{
                System.out.println(queryResult + "\n\n");
            }
            long timestamp_stop = System.currentTimeMillis();
            CustomLogger.info("("+(timestamp_stop-timestamp_start)+" milliseconds )");

            // Lexicon.clear();

        }
        scanner.close();

    }

    private static PostingList[] getPostingLists(List<String> terms) {
        PostingList [] postingLists = new PostingList[terms.size()];
        for(String term : terms){
            postingLists[terms.indexOf(term)] = new PostingList(term,false);
        }
        return postingLists;
    }
}

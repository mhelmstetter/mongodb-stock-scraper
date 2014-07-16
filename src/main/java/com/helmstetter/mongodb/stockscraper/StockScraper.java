package com.helmstetter.mongodb.stockscraper;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.joda.time.DateTime;
import org.joda.time.MutableDateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class StockScraper {

    protected static final Logger logger = LoggerFactory.getLogger(StockScraper.class);
    
    MongoClient mongoClient;
    private DBCollection tickerCollection;
    private DBCollection companyCollection;
    Set<String> symbolList;
    
    Calendar startDate;
    Calendar endDate;
    
    private Integer years;
    
    TickerScraper tickerScraper;
    CompanyScraper companyScraper;
    int tickerCount = 0;
    
    public void scrape() throws IOException {
        if (symbolList == null || symbolList.size() == 0) {
            scrapeAll();
        } else {
            scrape(symbolList);
        }
    }
    
    private void scrape(Set<String> symbols) throws IOException {
        companyScraper.scrape(symbolList);
        
        tickerCount = 0;
        for (String symbol : symbols) {
            scrape(startDate, endDate, symbol);
        }
        
    }

    private void scrapeAll() throws IOException {
        DBObject projection = new BasicDBObject("_id", 1);
        DBCursor cursor = companyCollection.find(null, projection);
        
        if (! cursor.hasNext()) {
            logger.debug(String.format("No companies in \"%s\" collection, scraping company list", companyCollection));
            companyScraper.scrape();
        }
        
        cursor = companyCollection.find(null, projection);
        tickerCount = 0;
        while (cursor.hasNext()) {
            DBObject companyDbo = cursor.next();
            String symbol = (String)companyDbo.get("_id");
            scrape(startDate, endDate, symbol);
        }
    }
    
    private void scrape(Calendar startDate, Calendar endDate, String symbol) {
        tickerScraper.scrape(startDate, endDate, symbol);
        tickerCount++;
        if (tickerCount % 1000 == 0) {
            logger.debug(String.format("Scraped %s stock quotes, current: %s", tickerCount, symbol));
        }
    }
    
    @SuppressWarnings("static-access")
    void initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(new Option( "help", "print this message" ));
        options.addOption(OptionBuilder.withArgName("hostname")
                .hasArg()
                .withDescription(  "mongod or mongos host (default localhost)" )
                .withLongOpt("host")
                .create( "h" ));
        options.addOption(OptionBuilder.withArgName("port number")
                .hasArg()
                .withDescription(  "mongod or mongos port (default 27017)" )
                .withLongOpt("port")
                .create( "p" ));
        options.addOption(OptionBuilder.withArgName("database name")
                .hasArg()
                .withDescription(  "database to use (default \"stock\")" )
                .create( "db" ));
        options.addOption(OptionBuilder.withArgName("collection name")
                .hasArg()
                .withDescription(  "company collection name (default \"company\")" )
                .withLongOpt("collection")
                .create( "c" ));
        options.addOption(OptionBuilder.withArgName("ticker collection")
                .hasArg()
                .withDescription(  "ticker collection to use (default \"company\")" )
                .withLongOpt("ticker-collection")
                .create( "t" ));
        options.addOption(OptionBuilder.withArgName("years")
                .hasArg()
                .withDescription(  "number of years of history to scrape (default 1)" )
                .withLongOpt("years")
                .create( "y" ));
        options.addOption(OptionBuilder.withArgName("symbol list")
                .hasArg()
                .withDescription(  "comma seperated list of ticker symbols to scrape" )
                .withLongOpt("symbols")
                .create( "s" ));
        

        CommandLineParser parser = new GnuParser();
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            if (line.hasOption("help")) {
                printHelpAndExit(options); 
            }
            initOptions(line);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            printHelpAndExit(options);
        } catch (Exception e) {
            e.printStackTrace();
            printHelpAndExit(options);
        }
    }

    private void initOptions(CommandLine line) throws UnknownHostException {
        Configuration props = new PropertiesConfiguration();
        props.addProperty("host", line.getOptionValue("h"));
        props.addProperty("port", line.getOptionValue("p"));
        props.addProperty("database", line.getOptionValue("db"));
        props.addProperty("years", line.getOptionValue("y"));
        props.addProperty("tickerCollection", line.getOptionValue("t"));
        props.addProperty("companyCollection", line.getOptionValue("c"));

        String symbolCsv = line.getOptionValue("s");
        if (symbolCsv != null) {
            props.addProperty("symbolList", symbolCsv.split(","));
        }

        String host = props.getString("host", "localhost");
        Integer port = props.getInt("port", 27017);
        String databaseName = props.getString("database", "stock");
        String tickerCollectionName = props.getString("tickerCollection", "ticker");
        String companyCollectionName = props.getString("companyCollection", "company");

        years = props.getInteger("years", 1);

        symbolList = new HashSet<String>(Arrays.asList(props.getStringArray("symbolList")));

        mongoClient = new MongoClient(host, port);
        DB db = mongoClient.getDB(databaseName);
        tickerCollection = db.getCollection(tickerCollectionName);
        companyCollection = db.getCollection(companyCollectionName);

        logger.debug(String.format("Loading data into database: %s, tickerCollection: %s, companyCollection: %s",
                databaseName, tickerCollectionName, companyCollectionName));
        
        

        tickerScraper = new TickerScraper(tickerCollection);
        companyScraper = new CompanyScraper(companyCollection);

        DateTime dt = new DateTime();
        MutableDateTime mdt = dt.toMutableDateTime();
        // perform various calculations on mdt
        mdt.addYears(years * -1);
        mdt.setMonthOfYear(1);
        mdt.setDayOfMonth(1);
        DateTime calculatedStartDate = mdt.toDateTime();
        startDate = calculatedStartDate.toGregorianCalendar();
        
        logger.debug(String.format("Scraping %s years of data, startDate: %s", years, DateTimeFormat.forPattern("dd/MM/YYYY").print(calculatedStartDate)));

        // End Date
        endDate = Calendar.getInstance();
    }

    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("scraper", options);
        System.exit(-1);
    }

    public static void main(String[] args) throws IOException {

        StockScraper scraper = new StockScraper();
        scraper.initializeAndParseCommandLineOptions(args);
        scraper.scrape();

    }

}

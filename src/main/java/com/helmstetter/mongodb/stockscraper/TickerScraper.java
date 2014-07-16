package com.helmstetter.mongodb.stockscraper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class TickerScraper {

    protected static final Logger logger = LoggerFactory.getLogger(TickerScraper.class);
    
    private final static String BASE_URL = "http://ichart.finance.yahoo.com/table.csv?s=";
    private final static String URL_FORMAT = BASE_URL + "%s&d=%s&e=%s&f=%s&g=d&a=%s&b=%s&c=%s";

    private DBCollection collection;

    public TickerScraper(DBCollection collection) {
        this.collection = collection;
    }

    public void scrape(Calendar startDate, Calendar endDate, String symbol) {
        URL url = null;
        HttpURLConnection urlConn = null;
        InputStreamReader inStream = null;
        
        try {
            if (logger.isTraceEnabled()) {
                logger.trace(String.format("scraping %s", symbol));
            }
            url = buildUrl(startDate, endDate, symbol);
            urlConn = (HttpURLConnection) url.openConnection();

            int code = urlConn.getResponseCode();
            if (code != 200) {
                throw new IOException("Response not 200 OK, code=" + code + ", url=" + url);
            }

            inStream = new InputStreamReader(urlConn.getInputStream());
            BufferedReader buff = new BufferedReader(inStream);
            String stringLine;

            buff.readLine(); // Read the firstLine. This is the header.

            while ((stringLine = buff.readLine()) != null) {
                String[] dohlcav = stringLine.split("\\,"); 
                String date = dohlcav[0];
                double open = Double.parseDouble(dohlcav[1]);
                double high = Double.parseDouble(dohlcav[2]);
                double low = Double.parseDouble(dohlcav[3]);
                double close = Double.parseDouble(dohlcav[4]);
                long volume = Long.parseLong(dohlcav[5]);
                double adjClose = Double.parseDouble(dohlcav[6]);

                DBObject stockDaily = new BasicDBObject();
                DBObject id = new BasicDBObject();
                id.put("sym", symbol);
                id.put("dt", date);
                stockDaily.put("_id", id);

                stockDaily.put("open", open);
                stockDaily.put("high", high);
                stockDaily.put("low", low);
                stockDaily.put("close", close);
                stockDaily.put("vol", volume);
                stockDaily.put("adjClose", adjClose);

                collection.save(stockDaily);
            }

        } catch (MalformedURLException e) {
            logger.error("url error", e);
        } catch (IOException e) {
            logger.error("io error", e);
        }
    }
    
    private final static URL buildUrl(Calendar startDate, Calendar endDate, String tickerSymbol) throws MalformedURLException {
        String startDateDay = Integer.toString(startDate.get(Calendar.DATE));
        String startDateMonth = Integer.toString(startDate.get(Calendar.MONTH));
        String startDateYear = Integer.toString(startDate.get(Calendar.YEAR));

        String endDateDay = Integer.toString(endDate.get(Calendar.DATE));
        String endDateMonth = Integer.toString(endDate.get(Calendar.MONTH));
        String endDateYear = Integer.toString(endDate.get(Calendar.YEAR));

        return new URL(String.format(URL_FORMAT, tickerSymbol, endDateMonth, endDateDay, endDateYear, startDateMonth, startDateDay, startDateYear));
    }

}
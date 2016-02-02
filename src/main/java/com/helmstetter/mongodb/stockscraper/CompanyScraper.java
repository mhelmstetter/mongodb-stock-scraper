package com.helmstetter.mongodb.stockscraper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class CompanyScraper {

    protected static final Logger logger = LoggerFactory.getLogger(CompanyScraper.class);
    
    private DBCollection collection;
    
    private final static String DEFAULT_URL = "http://finviz.com/export.ashx?v=111&&o=ticker";
    
    public CompanyScraper(DBCollection collection) {
        this.collection = collection;
    }
    
    private static void addStringAsDouble(DBObject dbo, String key, String doubleStr) {
        if (doubleStr != null && !doubleStr.equals("")) {
            dbo.put(key, Double.parseDouble(doubleStr));
        }
        
    }
    
    private static void addStringAsInteger(DBObject dbo, String key, String integerStr) {
        if (integerStr != null && !integerStr.equals("")) {
            dbo.put(key, Integer.parseInt(integerStr));
        }
        
    }
    
    public void scrape(Set<String> symbolFilterList) throws IOException {
        scrape(DEFAULT_URL, symbolFilterList);
    }
    
    public void scrape() throws IOException {
        scrape(DEFAULT_URL, null);
    }

    private void scrape(String urlStr, Set<String> symbolFilterList) throws IOException {
        logger.debug("Scraping companies, symbolFilterList=" + symbolFilterList);
        URL url = new URL(urlStr);
        HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
        int code = urlConn.getResponseCode();
        if (code != 200) {
            throw new IOException("Response not 200 OK, code=" + code);
        }
        logger.debug("Scrape response code: " + code);
        InputStreamReader inStream = new InputStreamReader(urlConn.getInputStream());
        BufferedReader buff = new BufferedReader(inStream);
        //String stringLine;

        
        CSVReader reader = new CSVReader(buff, ',', '"', 1);
        int companyCount = 0;
        String[] dohlcav;
        while ((dohlcav = reader.readNext()) != null) {
            DBObject company = new BasicDBObject();
            String symbol = dohlcav[1];
            if (symbolFilterList != null && !symbolFilterList.contains(symbol)) {
                continue;
            }
            
            int col = 1;
            company.put("_id", dohlcav[col++]);
            company.put("name", dohlcav[col++]);
            company.put("sector", dohlcav[col++]);
            company.put("industry", dohlcav[col++]);
            company.put("country", dohlcav[col++]);
            
            addStringAsDouble(company, "mcap", dohlcav[col++]);
            addStringAsDouble(company, "pe", dohlcav[col++]);
            addStringAsDouble(company, "price", dohlcav[col++]);
            addStringAsDouble(company, "change", dohlcav[col++].replace("%", ""));
            addStringAsInteger(company, "volume", dohlcav[col++]);

            collection.save(company);
            companyCount++;
        }
        reader.close();
        logger.debug(String.format("Loaded %s companies", companyCount));

    }

}

MongoDB Stock Scraper
=====================

This project provides a very simple web scraper that will collect stock data and load that stock data into MongoDB.

The scraper will collect "company" data and "ticker" data which are loaded into two separate collections.

Company Data
------------
The company data contains basic information about each company including market cap, PE, current price, change, and volume. By default,
this data is stored in the "company" collection. Here is an example document:

		{
			"_id" : "MSFT",
			"name" : "Microsoft Corporation",
			"sector" : "Technology",
			"industry" : "Business Software & Services",
			"country" : "USA",
			"mcap" : 350654.41,
			"pe" : 15.9,
			"price" : 43.05,
			"change" : 1.41,
			"volume" : 9017093
		}

Ticker Data
-----------
The ticker data contains daily stock prices including Open/High/Low/Close data. Here is an example document.

		{
			"_id" : {
				"sym" : "MSFT",
				"dt" : "2014-07-15"
			},
			"open" : 42.33,
			"high" : 42.47,
			"low" : 42.03,
			"close" : 42.45,
			"vol" : NumberLong(28748700),
			"adjClose" : 42.45
		}

Usage
-----
The distribution zip file contains unix and windows executable scripts for running the scraper. All command line options are optional, defaults are
provided within the usage below. 

		usage: scraper
		 -c,--collection <collection name>            company collection name
		                                              (default "company")
		 -db <database name>                          database to use (default
		                                              "stock")
		 -h,--host <hostname>                         mongod or mongos host
		                                              (default localhost)
		 -help                                        print this message
		 -p,--port <port number>                      mongod or mongos port
		                                              (default 27017)
		 -s,--symbols <symbol list>                   comma seperated list of
		                                              ticker symbols to scrape
		 -t,--ticker-collection <ticker collection>   ticker collection to use
		                                              (default "company")
		 -y,--years <years>                           number of years of history
		                                              to scrape (default 1)
		                                              
### Example Usage

Scrape and load the last 5 years of data for all companies:

		stock-scraper -y 5
		
Scrape and load the last 5 years of data for MSFT and ORCL:

		stock-scraper -s MSFT,ORCL -y 5
		                                              
                                              
                                              
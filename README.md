Scrapes all coin data from a popular coin tracker

Designed to be run on a schedule and pull data into a data warehouse

The data warehouse stores the data in it's raw format giving you the ability to post-process the complex data

There is a script to post-process the data, I use the data warehouse API to get my data and build a pandas dataframe that i then pass into this post-processing class

Example output:

```json
[
    {
        "ath": 5.6900200843811035,
        "atl": 0.0796197338733,
        "circulatingSupply": 3210367601,
        "cmcRank": 101,
        "dateAdded": "2017-06-13T00:00:00.000Z",
        "high24h": 0.309033957151786,
        "id": 1720,
        "isActive": 1,
        "isAudited": false,
        "lastUpdated": "2024-04-07T11:54:00.000Z",
        "low24h": 0.29901754541462855,
        "marketPairCount": 138,
        "maxSupply": "IOTA",
        "name": 0.0383,
        "quote.USD.dominance": 992021721.05,
        "quote.USD.fullyDilluttedMarketCap": "2024-04-07T11:54:00.000Z",
        "quote.USD.lastUpdated": 992021721.0512946,
        "quote.USD.marketCap": 992021721.0512946,
        "quote.USD.marketCapByTotalSupply": "USD",
        "quote.USD.name": 0.04334556,
        "quote.USD.percentChange1h": 35.7131539,
        "quote.USD.percentChange1y": 2.41374736,
        "quote.USD.percentChange24h": -7.97434335,
        "quote.USD.percentChange30d": 27.37920118,
        "quote.USD.percentChange60d": -11.18429892,
        "quote.USD.percentChange7d": 30.13045533,
        "quote.USD.percentChange90d": 0.3090056480579635,
        "quote.USD.price": 0,
        "quote.USD.selfReportedMarketCap": 0.00923738,
        "quote.USD.turnover": 9163683.13160527,
        "quote.USD.volume24h": 284681737.40114045,
        "quote.USD.volume30d": 67066255.62866819,
        "quote.USD.volume7d": -1.9801,
        "quote.USD.ytdPriceChangePercentage": 101,
        "rank": 0,
        "selfReportedCirculatingSupply": "iota",
        "slug": "IOTA",
        "symbol": 3210367601,
        "totalSupply": null,
        "tvl": null,
        "pk": "2024-04-07-page-2-idx-0"
    } 
]

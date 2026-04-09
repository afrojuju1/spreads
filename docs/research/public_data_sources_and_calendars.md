# Public Data Sources And Calendars

Verified on April 9, 2026.

Purpose: identify free or public data sources that can feed the system alongside Alpaca, with emphasis on event calendars, context layers, fundamentals, and research infrastructure.

## Current Repo State

The repo already has a calendar-events subsystem and three adapters:

- [earnings_calendar.py](/Users/adeb/Projects/spreads/src/spreads/integrations/calendar_events/adapters/earnings_calendar.py)
- [macro_calendar.py](/Users/adeb/Projects/spreads/src/spreads/integrations/calendar_events/adapters/macro_calendar.py)
- [alpaca_corporate_actions.py](/Users/adeb/Projects/spreads/src/spreads/integrations/calendar_events/adapters/alpaca_corporate_actions.py)

Current posture:

- macro calendar is effectively local-file driven
- earnings currently uses a low-confidence DoltHub source
- corporate actions already use Alpaca

So the biggest improvement opportunity is not more Alpaca research. It is adding stronger public context and event feeds around Alpaca.

## Best Public Sources

## 1. SEC EDGAR

Best overall public source for filings, filing-driven catalysts, and fundamental context.

What it gives:

- real-time submissions updates
- company submissions history
- XBRL company facts
- nightly bulk ZIPs for `submissions` and `companyfacts`

Best uses in this system:

- `8-K` catalyst detection
- `10-Q` / `10-K` filing context
- insider / ownership / filing event overlays
- issuer-level fundamental and disclosure context

Why it matters:

- this is the strongest free source for company-specific event intelligence
- it complements Alpaca instead of duplicating it

Practical note:

- SEC says the public data APIs do not require API keys
- SEC also says bulk archives are the most efficient way to pull large amounts of data

## 2. Official Macro Calendar Stack

Best public event-calendar opportunity for the repo.

Use official calendars instead of maintaining a mostly manual macro schedule.

Recommended stack:

- BLS release calendar and `.ics`
- BEA release pages and API
- Census economic indicator calendar
- Federal Reserve FOMC calendar
- Treasury auction schedule and FiscalData auction dataset

Best uses in this system:

- macro event blocking or penalization
- premarket playbook context
- session-risk tagging
- catalyst and event intelligence layer

Why it matters:

- these are official schedules
- they directly improve the current calendar subsystem
- they are more trustworthy than ad hoc files for important macro dates

## 3. FRED

Best public source for macro series, benchmark series, and historical economic context.

What it gives:

- broad macro and rates series
- standardized access to many economic datasets
- easy downstream use for relative and regime research

Best uses in this system:

- macro overlays
- rates and volatility regime context
- benchmark series for evaluation and relative analysis

Why it matters:

- not a primary event source
- very useful as a research and evaluation reference layer

## 4. FINRA Transparency Data

Best public source for short-sale and off-exchange transparency context.

What it gives:

- Reg SHO daily short-sale volume
- ATS and OTC weekly summaries
- short-interest and related transparency datasets

Best uses in this system:

- short-pressure context
- participation and sentiment overlays
- additional research context for stock-led moves

Why it matters:

- this is one of the better public transparency layers you can add without paying for deeper market structure
- useful for system-wide context, not just a single feature

Practical note:

- FINRA access is through its developer platform and terms
- treat it as public/licensed data rather than “frictionless no-auth bulk”

## 5. CFTC Commitments Of Traders

Best public medium-horizon positioning source.

What it gives:

- weekly COT reports
- historical compressed annual files
- positioning across futures and options on futures

Best uses in this system:

- medium-horizon macro positioning context
- ETF/index proxy background state
- regime overlays

Why it matters:

- not useful for intraday triggers
- useful as a slower-moving research input

## 6. Treasury FiscalData

Best public Treasury-market and issuance context source.

What it gives:

- auction datasets
- issuance timing and auction details
- JSON/CSV/XML access through FiscalData

Best uses in this system:

- Treasury-auction event calendar
- rates-event risk context
- macro calendar enrichment

Why it matters:

- this is a real official API, not just scraped schedule pages

## Additional Non-Calendar Sources

These are valuable for the system as a whole even when they are not primarily event calendars.

## 7. SEC Structured Market And Holdings Datasets

Beyond EDGAR filings, the SEC Data Library exposes several useful public datasets.

Most useful ones for this system:

- `13F` holdings datasets
- financial statement and notes datasets
- fails-to-deliver data
- `company_tickers.json` and `company_tickers_exchange.json`
- `N-PORT` fund holdings datasets

Best uses in this system:

- holdings and ownership context
- ETF and manager exposure research
- symbol master and identifier bootstrapping
- settlement-stress and short-pressure context through fails-to-deliver

Why it matters:

- this is one of the richest free structured U.S. market-data libraries outside broker market-data APIs
- it is especially useful for research, context, and normalization

## 8. GDELT

Best public source for broad news and event coverage beyond Alpaca's built-in Benzinga stream.

What it gives:

- global event database
- global knowledge graph
- quotation graph
- geographic graph
- realtime and historical open datasets

Best uses in this system:

- broader catalyst discovery
- theme detection and narrative clustering
- entity co-mention and relationship context
- market-moving coverage outside traditional finance-only wires

Why it matters:

- Alpaca news is useful, but still limited compared with a broader open news graph
- GDELT is more valuable as a research and enrichment source than as a primary trigger source

Practical note:

- GDELT updates frequently and is very broad, so it needs filtering and entity-resolution work before it becomes operationally useful

## 9. OpenFIGI

Best public identifier-normalization source.

What it gives:

- ticker and identifier mapping to FIGI
- free public API with lower rate limits without an API key

Best uses in this system:

- cross-source security mapping
- symbol and exchange normalization
- downstream data-linking across SEC, Alpaca, FINRA, and future sources

Why it matters:

- once multiple sources are ingested, identifier quality becomes a real system problem
- OpenFIGI is not a signal source, but it is useful infrastructure

## 10. FINRA TRACE Treasury Aggregates

Useful public rates-market context source.

What it gives:

- Treasury daily aggregates
- Treasury monthly aggregates

Best uses in this system:

- rates-liquidity background context
- macro and Treasury-auction follow-through research
- session-risk overlays for rate-sensitive names and ETFs

Why it matters:

- this is not an equity/options trigger source
- it is still useful as a system-wide macro context feed

## Weak Spots And Cautions

## Earnings Calendars

This is still the weak point in the free stack.

Current repo state:

- [earnings_calendar.py](/Users/adeb/Projects/spreads/src/spreads/integrations/calendar_events/adapters/earnings_calendar.py) uses a low-confidence DoltHub earnings source

Practical conclusion:

- there is no strong official public U.S. earnings calendar source that is clean enough to treat as system-of-record
- keep earnings as lower-confidence context unless a better source is added later

## Corporate Actions

Alpaca corporate actions are already useful, but they can lag provider timing.

Practical conclusion:

- keep Alpaca corporate actions
- supplement them with SEC filing ingestion for stronger event awareness

## What To Prioritize

If the goal is to improve the whole system, this is the order:

1. official macro calendar ingestion
2. SEC filings ingestion
3. Treasury and Fed event ingestion
4. FINRA transparency layer
5. FRED macro reference layer

If the goal is broader system enrichment after that, this is the next order:

1. SEC structured datasets beyond raw filings
2. GDELT for broader catalyst and narrative context
3. OpenFIGI for identifier normalization
4. FINRA TRACE Treasury aggregates for rates context

## Best Repo Fit

These are the highest-value additions to this codebase:

- replace or augment the local macro calendar path with official BLS, BEA, Census, Fed, and Treasury ingesters
- keep Alpaca corporate actions as-is, but add SEC filing ingestion for richer event intelligence
- keep the current earnings adapter low-confidence unless a stronger source is adopted
- add FINRA transparency data as a research/context layer, not as a core real-time trigger source
- use FRED as benchmark and macro context for evaluation, regime, and relative-strength work
- add SEC structured datasets for holdings, symbol master, and fails-to-deliver research
- use GDELT as a broad enrichment layer when Alpaca news coverage is too narrow
- add OpenFIGI only when source-normalization pain becomes real

## Recommendation

Use Alpaca as the live market data backbone.

Use public sources for:

- event calendars
- filing-driven catalysts
- macro context
- transparency overlays
- research benchmarks

Do not try to replace Alpaca with public sources for live stock/options market data. That is the wrong trade.

## Sources

- [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
- [SEC data.sec.gov](https://data.sec.gov/)
- [BLS Developers](https://www.bls.gov/developers/)
- [BLS Release Calendar](https://www.bls.gov/schedule/news_release/default.asp)
- [Census Economic Indicators Calendar](https://www.census.gov/economic-indicators/calendar-listview.html)
- [BEA API](https://apps.bea.gov/api/signup/activate.html)
- [BEA Current Releases](https://www.bea.gov/news/current-releases)
- [FOMC Calendars](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm)
- [Treasury Securities Auctions Data](https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/treasury-securities-auctions-data)
- [FiscalData API Documentation](https://fiscaldata.treasury.gov/api-documentation/)
- [FRED API Terms](https://fred.stlouisfed.org/docs/api/terms_of_use.html)
- [SEC Data Library](https://www.sec.gov/data-research/sec-markets-data)
- [Accessing EDGAR Data](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data)
- [Company Tickers JSON](https://www.sec.gov/file/company-tickers)
- [GDELT Data](https://www.gdeltproject.org/data.html)
- [GDELT DOC 2.0 API](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/)
- [OpenFIGI API Documentation](https://www.openfigi.com/api/documentation)
- [FINRA API Docs](https://developer.finra.org/docs)
- [FINRA Reg SHO Daily Short Sale Volume](https://developer.finra.org/docs/api-explorer/query_api-equity-reg_sho_daily_short_sale_volume)
- [FINRA TRACE Treasury Aggregates](https://developer.finra.org/news/new-trace-treasury-daily-and-monthly-aggregates-dataset)
- [FINRA Equity Data Terms](https://developer.finra.org/sites/default/files/2022-12/Developer%20API%20-%20Specific%20Terms%20-%20Equity%20Data%20%2812-2022%29%5B10%5D.pdf)
- [CFTC Commitments of Traders](https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm)
- [CFTC Historical Compressed Files](https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm)

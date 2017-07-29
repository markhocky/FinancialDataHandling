'''
Created on 6 Dec 2014

@author: Mark
'''

import pandas_datareader
import pickle
import quandl
import os
import re
import pandas as pd
from datetime import date
from pandas import DataFrame

from data_types.market import Market
from data_types.filter_data import StackedFilterValues, WideFilterValues


def getMarket(type = "ASX", excluded_tickers = None):
    market_sources = {
        "ASX" : r'D:\Investing\Workspace\market_instruments.pkl', 
        "NYSE" : r'D:\Investing\Workspace\nyse_instruments.pkl', 
        "NYSE VALUE" : r'D:\Investing\Workspace\nyse_value_instruments.pkl'
        }

    source = market_sources[type.upper()]
    instruments = pd.read_pickle(source)
    if excluded_tickers is not None:
        tickers = list(set(instruments.items) - set(excluded_tickers))
        instruments = instruments.loc[tickers, :, :]
    else:
        tickers = list(instruments.items)
    start = instruments.iloc[0].index.min().to_pydatetime().date()
    end = instruments.iloc[0].index.max().to_pydatetime().date()
    market = Market(tickers, start, end)
    market.instruments = instruments
    market.exchange = type.upper().split()[0]
    return market

def getValues(type = None, exchange = "ASX"):
    data_source = {
        "ASX" : r'D:\Investing\Workspace\Valuations20170129.xlsx',
        "NYSE" : r'D:\Investing\Workspace\NYSEvaluations20170527.xlsx'
        }
    source = data_source[exchange]
    vals = pd.read_excel(source, index_col = 0)
    if type is not None:
        vals = vals[["ticker", type]]
    return StackedFilterValues(vals, type)

def getValueRatios(type, strat):
    valuation = getValues(type, strat.market.exchange).as_wide_values()
    ratios = valuation.values / strat.get_trade_prices()
    return WideFilterValues(ratios, name = "Value Ratio")

def getValueMetrics(type = None):
    vals = pd.read_excel(r'D:\Investing\Workspace\ValueMetrics20170329.xlsx', index_col = 0)
    if type is not None:
        vals = vals[["ticker", type]]
    return StackedFilterValues(vals, type)


class Handler(object):
    '''
    Handler uses the Pandas data functionality to download data and handle local storage.
    '''

    def __init__(self, location, exchange = "ASX"):
        '''
        Constructor
        '''
        self.location = location
        self.exchange = exchange

        
    def get(self, ticker, start, end):
        return pandas_datareader.get_data_yahoo(ticker + ".AX", start, end)


    def build_path(self, ticker):
        return os.path.join(self.location, self.exchange, ticker, ticker + "prices.pkl")
    
    def save(self, instrument, ticker):
        with open(self.build_path(ticker), "wb") as file:
            pickle.dump(instrument, file)
        
    def load(self, ticker, start = None, end = None):
        with open(self.build_path(ticker), "rb") as file:
            instrument = pickle.load(file)
        return self.adjust(instrument[start:end])

    def adjust(self, instrument):
        instrument = self.clean_adj_close(instrument)
        adjusted_data = DataFrame(index = instrument.index, columns = ["Open", "High", "Low", "Close", "Volume"], dtype = float)
        adjust_ratios = instrument["Adj Close"] / instrument["Close"]
        adjusted_data.Open = instrument["Open"] * adjust_ratios
        adjusted_data.High = instrument["High"] * adjust_ratios
        adjusted_data.Low = instrument["Low"] * adjust_ratios
        adjusted_data.Close = instrument["Close"] * adjust_ratios
        adjusted_data.Volume = instrument["Volume"]
        return adjusted_data

    
    def clean_adj_close(self, instrument):
        '''
        Takes a dataframe [OHLCV & Adj Close] for a ticker
        Tries to find any erroneous Adj Close values caused by stock splits.
        '''
        adj_ratios = instrument["Adj Close"] / instrument["Adj Close"].shift(1)
        close_ratios = instrument["Close"] / instrument["Close"].shift(1)
        limit = 3.0
        possible_errors = adj_ratios > limit
        while any(possible_errors):
            try:
                start = adj_ratios[possible_errors].index[0]
                ix = 0
                end = adj_ratios[adj_ratios < (1 / limit)].index[ix]
                while end < start:
                    ix += 1
                    end = adj_ratios[adj_ratios < (1 / limit)].index[ix]
            except IndexError:
                possible_errors[start] = False
            else:
                if (1 / limit) < close_ratios[end] < limit:
                    # Indicates Close is out of sync with Adj Close
                    divisor = round(adj_ratios[start])
                    instrument["Adj Close"][start:(end - DateOffset(1))] = instrument["Adj Close"][start:(end - DateOffset(1))] / divisor
                    adj_ratios = instrument["Adj Close"] / instrument["Adj Close"].shift(1)
                    possible_errors = adj_ratios > limit
                else:
                    # may be a genuine spike in the data
                    possible_errors[start] = False
        return instrument

    def download_market(self, tickers):
        market = Market(tickers, self.start, self.end)
        market.exchange = self.exchange
        for ticker in tickers:
            raw = self.get(ticker, self.start, self.end)
            market[ticker] = self.adjust(raw)
        return market

    def load_market(self, tickers):
        market = Market(tickers, self.start, self.end)
        market.exchange = self.exchange
        for ticker in self.tickers:
            market[ticker] = self.load(ticker, self.start, self.end)
        return market
    

class quandlAPI(Handler):

    def __init__(self, location = r"D:\Investing\Data", exchange = "NYSE"):
        super().__init__(location, exchange)
        with open(r'D:\Investing\Data\_keys\quandl.pkl', 'rb') as quandl_key:
            quandl.ApiConfig.api_key = pickle.load(quandl_key)

    def get(self, ticker, start, end):
        if isinstance(start, date):
            start = start.strftime("%Y-%m-%d")
        if isinstance(end, date):
            end = end.strftime("%Y-%m-%d")
        return quandl.get("WIKI/" + ticker, start_date = start, end_date = end)

    def adjust(self, instrument):
        instrument_adj = instrument[["Adj. Open", "Adj. High", "Adj. Low", "Adj. Close", "Adj. Volume"]]
        instrument_adj.columns = ["Open", "High", "Low", "Close", "Volume"]
        return instrument_adj

    def load(self, ticker, start = None, end = None):
        return super().load(ticker, start, end)


class PriceDownloader():

    def __init__(self, handler):

        self.handler = handler

    def download_and_save(self, tickers, start = "2005-01-01", end = None):
        if end is None:
            end = date.today()
        count = 0
        errors = {}
        for ticker in tickers:
            count += 1
            if count % 100 == 0:
                print("Saving {} of {}...".format(count, len(tickers)))
            # Skip tickers which are not ordinary shares
            if re.search("[/^/.]", ticker) is not None:
                continue
            try:
                data = self.handler.get(ticker, start, end)
                self.handler.save(data, ticker)
            except quandl.errors.quandl_error.NotFoundError:
                errors[ticker] = "Not found in quandl DB"
            except Exception as E:
                errors[ticker] = "Unhandled: {}".format(E)
        return errors



class YahooDataDownloader():
    '''
    Uses the Pandas data functionality to download data.
    '''
    def priceHistory(self, ticker, start = None, end = None):
        if start is None:
            start = datetime.date(2010, 1, 1)
        if end is None:
            end = datetime.date.today()
        return pd_data.get_data_yahoo(ticker + ".AX", start, end)

    def currentPrice(self, ticker):
        ticker = ticker + ".AX"
        quote = pd_data.get_quote_yahoo(ticker)
        return quote["last"][ticker]


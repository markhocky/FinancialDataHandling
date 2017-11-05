
import pandas

from formats import StorageResource


class Instruments(StorageResource):

    def __init__(self, name):
        self.name = name
        self.data = None
        self.start = None
        self.end = None
        
    def select_folder(self, store):
        self.exchange = store.exchange
        return store.workspace(self)

    def filename(self):
        return self.name.lower() + "_instruments.pkl"

    def load_from(self, file_path):
        self.data = pandas.read_pickle(file_path)
        self.start = self.data.iloc[0].index.min().to_pydatetime().date()
        self.end = self.data.iloc[0].index.max().to_pydatetime().date()
        return self

    def save_to(self, file_path):
        self.data.to_pickle(file_path)

    @property
    def tickers(self):
        return list(self.data.items)

    def exclude(self, excluded_tickers):
        '''
        Removes the specified tickers from instrument set.
        '''
        tickers = list(set(self.tickers) - set(excluded_tickers))
        new_set = Instruments(self.name)
        new_set.exchange = self.exchange
        new_set.start = self.start
        new_set.end = self.end
        new_set.data = self.data.loc[tickers, :, :]
        return new_set

    def include_only(self, included_tickers):
        '''
        Removes tickers which are not in the provided ticker set
        '''
        tickers = list(set(included_tickers).intersection(set(self.tickers)))
        new_set = Instruments(self.name)
        new_set.exchange = self.exchange
        new_set.start = self.start
        new_set.end = self.end
        new_set.data = self.data.loc[tickers, :, :]
        return new_set

    def up_to(self, end_date):
        '''
        Returns a new instruments object with a revised (shorter) end date.
        '''
        new_set = Instruments(self.name)
        new_set.exchange = self.exchange
        new_set.start = self.start
        new_set.data = self.data.loc[:, :, :end_date]
        new_set.end = self.data.iloc[0].index.max().to_pydatetime().date()
        return new_set


# TODO - provide methods for updating the data (i.e. for removing errors).
# TODO - consider adding a merge method for price history to append new data rather than overwrite.
class PriceHistory(StorageResource):
    
    def __init__(self, ticker):
        self.ticker = ticker
        self.prices = None

    def select_folder(self, store):
        return store.price_history(self)

    def filename(self):
        return self.ticker + "prices.pkl"

    def load_from(self, file_path):
        self.prices = pandas.read_pickle(file_path)
        return self

    def save_to(self, file_path):
        self.prices.to_pickle(file_path)


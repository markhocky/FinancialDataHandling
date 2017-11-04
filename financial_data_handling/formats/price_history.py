
import pandas

from formats import StorageResource


class Instruments(StorageResource):

    def __init__(self, name):
        self.name = name
        
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
        tickers = list(set(self.tickers) - set(excluded_tickers))
        self.data = self.data.loc[tickers, :, :]


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


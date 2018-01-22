import requests
import os
import shutil
import pandas
import datetime
import pickle
from pandas_datareader import data as pd_data
from pandas_datareader import base as pd_base
from bs4 import BeautifulSoup

from formats import StorageResource

class Financials(StorageResource):

    def __init__(self, ticker, period):
        self.ticker = ticker
        self.period = period.lower()
        self.statements = {}

    def merge(self, other):
        self.confirm_match(other.ticker, other.period)
        for sheet in other.statements:
            try:
                existing_sheet = self.statements[sheet]
            except KeyError:
                self.statements[sheet] = other.statements[sheet]
            else:
                new_sheet = other.statements[sheet]
                for table in new_sheet:
                    existing_table = existing_sheet[table]
                    new_table = new_sheet[table]
                    joined_table = self.merge_columns(existing_table, new_table)
                    self.statements[sheet][table] = joined_table

    def merge_columns(self, existing, new):
        existing_years = existing.columns.tolist()
        new_years = new.columns.tolist()
        append_years = [year not in new_years for year in existing_years]
        return pandas.concat([new, existing.iloc[:, append_years]], axis = 1)

    def confirm_match(self, ticker, period):
        if ticker != self.ticker or period != self.period:
            raise ValueError("Ticker and Period must match")

    def select_folder(self, store):
        return store.financials(self)

    def filename(self):
        return self.ticker + self.period + ".pkl"
 
    def save_to(self, file_path):
        with open(file_path, "wb") as file:
            pickle.dump(self.to_dict(), file)

    def load_from(self, file_path):
        with open(file_path, "rb") as file:
            dictionary = pandas.read_pickle(file)
        self.from_dict(dictionary)
        return self

    def to_dict(self):
        return {"ticker" : self.ticker,
                "period" : self.period, 
                "statements" : self.statements}

    def from_dict(self, dictionary):
        ticker = dictionary["ticker"]
        period = dictionary["period"].lower()
        self.confirm_match(ticker, period)
        self.statements = dictionary["statements"]

    @property
    def income(self):
        return self.statements["income"]["income"]

    @property
    def assets(self):
        return self.statements["balance"]["assets"]

    @property
    def liabilities(self):
        return self.statements["balance"]["liabilities"]

    @property
    def operating(self):
        return self.statements["cashflow"]["operating"]

    @property
    def financing(self):
        return self.statements["cashflow"]["financing"]

    @property
    def investing(self):
        return self.statements["cashflow"]["investing"]

    def last_year(self):
        last_period = self.income.columns[0]
        if self.period == "annual":
            last_date = datetime.datetime.strptime(last_period, "%Y")
        elif self.period == "interim":
            last_date = datetime.datetime.strptime(last_period, "%d-%b-%Y")
        else:
            raise AttributeError("Period must be annual or interim.")
        return last_date.year

    def num_columns(self):
        return len(self.income.columns)


class StatementWebpage(StorageResource):

    def __init__(self, ticker, type, period):
        self.ticker = ticker
        self.type = type
        self.period = period
        self.html = None

    def select_folder(self, store):
        if self.period is "annual":
            return store.annual_financials(self)
        else:
            return store.interim_financials(self)

    def filename(self):
        return self.ticker + self.type + ".html"

    def load_from(self, file_path):
        with open(file_path, 'r') as file:
            self.html = file.read()
        return self

    def save_to(self, file_path):
        with open(file_path, 'w') as file:
            file.write(str(self.html))


class ValuationSummary(StorageResource):

    def __init__(self, date = "*"):
        # Assumes date in YYYYMMDD format
        # If no date was provided then Storage will attempt to find the latest.
        self.date = date
        self.data = None

    def select_folder(self, store):
        return store.valuation_summary(self)

    def filename(self):
        return "ValuationSummary" + self.date + ".xlsx"

    def load_from(self, file_path):
        self.data = pandas.read_excel(file_path, index_col = 0)
        return self

    def save_to(self, file_path):
        self.data.to_excel(file_path)


class StackedValuations(ValuationSummary):
    """
    StackedValuations represents the tabular data with values for each ticker
    stacked on top of each other. The table may contain multiple sub-types,
    e.g. for Valuations may contain; Min, Max, Base..
    This class also provides the method to convert back to a standard (pivoted)
    dataframe with a column for each ticker.
    """
    
    def __getitem__(self, key):
        return self.data[self.data.ticker == key][self.data.columns[1:]]

    @property
    def types(self):
        return self.data.columns[1:].tolist()

    def as_wide_values(self, type = None, index = None):
        if type is None or type not in self.types:
            raise TypeError("A type is required. Select one of: {0}".format(", ".join(self.types)))
        df = self.data.copy()
        df['date'] = df.index
        df = df.pivot(index = 'date', columns = 'ticker', values = type)
        df = df.fillna(method = 'ffill')
        if index is not None:
            df = df.reindex(index, method = 'ffill')
        return df


class Valuations(StackedValuations):

    def filename(self):
        return "Valuations" + self.date + ".xlsx"

class ValuationMetrics(StackedValuations):

    def filename(self):
        return "ValuationMetrics" + self.date + ".xlsx"


class AnalysisSummary(StorageResource):

    def __init__(self, reporter):
        self.ticker = reporter.ticker
        self.summary = reporter.summary_table()
        self.reporter = reporter

    def select_folder(self, store):
        return store.analysis_summary(self)

    def filename(self):
        return self.ticker + "analysis.xlsx"

    def save_to(self, file_path):
        writer = pandas.ExcelWriter(file_path)
        self.summary.to_excel(writer, "Summary")
        self.reporter.financials_to_excel(writer)
        writer.save()


class CMChistoricals(StorageResource):

    def __init__(self, ticker):
        self.ticker = ticker
        self.summary = None

    def select_folder(self, store):
        return store.CMCsummary(self)

    def filename(self):
        return self.ticker + "historical.pkl"

    def save_to(self, file_path):
        self.summary.to_pickle(file_path)

    def load_from(self, file_path):
        self.summary = pandas.read_pickle(file_path)
        return self


class CMCpershare(StorageResource):

    def __init__(self, ticker):
        self.ticker = ticker
        self.summary = None

    def select_folder(self, store):
        return store.CMCsummary(self)

    def filename(self):
        return self.ticker + "pershare.pkl"

    def save_to(self, file_path):
        return self.summary.to_pickle(file_path)

    def load_from(self, file_path):
        self.summary = pandas.read_pickle(file_path)
        return self


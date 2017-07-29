import requests
import os
import shutil
import pandas
import datetime
import pickle
from pandas_datareader import data as pd_data
from pandas_datareader import base as pd_base
from bs4 import BeautifulSoup


class StorageResource():

    def selectFolder(self, store):
        raise NotImplementedError

    def filename(self):
        raise NotImplementedError

    def loadFrom(self, file_path):
        raise NotImplementedError

    def saveTo(self, file_path):
        raise NotImplementedError


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

    def selectFolder(self, store):
        return store.financials(self)

    def filename(self):
        return self.ticker + self.period + ".pkl"
 
    def saveTo(self, file_path):
        with open(file_path, "wb") as file:
            pickle.dump(self.to_dict(), file)

    def loadFrom(self, file_path):
        with open(file_path, "rb") as file:
            dictionary = pickle.load(file)
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

    def lastYear(self):
        last_period = self.income.columns[0]
        if self.period == "annual":
            last_date = datetime.datetime.strptime(last_period, "%Y")
        elif self.period == "interim":
            last_date = datetime.datetime.strptime(last_period, "%d-%b-%Y")
        else:
            raise AttributeError("Period must be annual or interim.")
        return last_date.year

    def numColumns(self):
        return len(self.income.columns)


class StatementWebpage(StorageResource):

    def __init__(self, ticker, type, period):
        self.ticker = ticker
        self.type = type
        self.period = period
        self.html = None

    def selectFolder(self, store):
        if self.period is "annual":
            return store.annualFinancials(self)
        else:
            return store.interimFinancials(self)

    def filename(self):
        return self.ticker + self.type + ".html"

    def loadFrom(self, file_path):
        with open(file_path, 'r') as file:
            self.html = file.read()
        return self

    def saveTo(self, file_path):
        with open(file_path, 'w') as file:
            file.write(str(self.html))


class PriceHistory(StorageResource):
    
    def __init__(self, ticker):
        self.ticker = ticker
        self.prices = None

    def selectFolder(self, store):
        return store.priceHistory(self)

    def filename(self):
        return self.ticker + "prices.pkl"

    def loadFrom(self, file_path):
        self.prices = pandas.read_pickle(file_path)
        return self

    def saveTo(self, file_path):
        self.prices.to_pickle(file_path)


class ValuationSummary(StorageResource):

    def __init__(self, date):
        # Assumes date in YYYYMMDD format
        self.date = date
        self.summary = None

    def selectFolder(self, store):
        return store.valuationSummary(self)

    def filename(self):
        return "ValuationSummary" + self.date + ".xlsx"

    def loadFrom(self, file_path):
        self.summary = pandas.read_excel(file_path, index_col = 0)
        return self

    def saveTo(self, file_path):
        self.summary.to_excel(file_path)


class Valuations(ValuationSummary):

    def filename(self):
        return "Valuations" + self.date + ".xlsx"


class AnalysisSummary(StorageResource):

    def __init__(self, reporter):
        self.ticker = reporter.ticker
        self.summary = reporter.summaryTable()
        self.reporter = reporter

    def selectFolder(self, store):
        return store.analysisSummary(self)

    def filename(self):
        return self.ticker + "analysis.xlsx"

    def saveTo(self, file_path):
        writer = pandas.ExcelWriter(file_path)
        self.summary.to_excel(writer, "Summary")
        self.reporter.financialsToExcel(writer)
        writer.save()


class CMChistoricals(StorageResource):

    def __init__(self, ticker):
        self.ticker = ticker
        self.summary = None

    def selectFolder(self, store):
        return store.CMCsummary(self)

    def filename(self):
        return self.ticker + "historical.pkl"

    def saveTo(self, file_path):
        self.summary.to_pickle(file_path)

    def loadFrom(self, file_path):
        self.summary = pandas.read_pickle(file_path)
        return self


class CMCpershare(StorageResource):

    def __init__(self, ticker):
        self.ticker = ticker
        self.summary = None

    def selectFolder(self, store):
        return store.CMCsummary(self)

    def filename(self):
        return self.ticker + "pershare.pkl"

    def saveTo(self, file_path):
        return self.summary.to_pickle(file_path)

    def loadFrom(self, file_path):
        self.summary = pandas.read_pickle(file_path)
        return self


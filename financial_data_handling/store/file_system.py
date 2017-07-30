
import requests
import os
import shutil
import pandas
import datetime
import pickle
from pandas_datareader import data as pd_data
from pandas_datareader import base as pd_base
from bs4 import BeautifulSoup

from formats.price_history import Instruments
from formats.fundamentals import Valuations, ValuationMetrics

class XLSio():

    def __init__(self, store):
        self.store = store

    # NOTE: This is currently working on the assumption that the workbook only
    # contains one worksheet.
    # Takes inputs from ASX Listed Companies downloaded from ASX.com.au
    def load_workbook(self, name):
        if name is "ASXListedCompanies.xlsx":
            header = 2
        else:
            header = 0
        table = pandas.read_excel(os.path.join(self.store.data, name), header = header)
        table.index = table.pop("ASX code")
        self.table = table

    def get_header(self):
        return self.table.columns.tolist()

    def get_tickers(self):
        return self.table.index.tolist()

    def update_table(self, new_data):
        new_table = pandas.DataFrame.from_dict(new_data, orient = "index")
        self.table = self.table.join(new_table)

    def save_as(self, filename):
        self.table.to_excel(os.path.join(self.store.data, filename), sheet_name = "Stock table")


class Storage():
    
    def __init__(self, exchange = "ASX", root_folder = "D:\\Investing\\"):
        self.root = root_folder
        self.exchange = exchange

    @property
    def data(self):
        return os.path.join(self.root, "Data", self.exchange)

    @property
    def valuations(self):
        return os.path.join(self.root, "Valuations", self.exchange)

    def workspace(self, resource):
        return os.path.join(self.root, "Workspace")

    def load(self, resource):
        folder = resource.select_folder(self)
        filename = resource.filename()
        return resource.load_from(os.path.join(folder, filename))

    def save(self, resource):
        folder = resource.select_folder(self)
        self.check_directory(folder)
        file_path = os.path.join(folder, resource.filename())
        resource.save_to(file_path)

    def stock_folder(self, resource):
        return os.path.join(self.data, resource.ticker)

    def financials(self, resource):
        return os.path.join(self.stock_folder(resource), "Financials")

    def CMCsummary(self, resource):
        return self.financials(resource)

    def annual_financials(self, resource):
        return os.path.join(self.stock_folder(resource), "Financials", "Annual")

    def interim_financials(self, resource):
        return os.path.join(self.stock_folder(resource), "Financials", "Interim")

    def price_history(self, resource):
        return self.stock_folder(resource)

    def analysis_summary(self, resource):
        return self.stock_folder(resource)

    def valuation_summary(self, resource):
        return self.valuations

    def check_directory(self, path):
        if "." in os.path.basename(path):
            path = os.path.dirname(path)
        if not os.path.exists(path):
            os.makedirs(path)

    def list_files(self, root_dir, search_term = ""):
        all_files = os.listdir(root_dir)
        return [filename for filename in all_files if search_term in filename]

    def migrate_all(self, folder_pattern, type, tickers = None, file_pattern = None):

        if tickers is None:
            xls = XLSio(self)
            xls.load_workbook("StockSummary")
            xls.table = xls.table[xls.table["P/E Ratio (TTM)"].notnull()]
            tickers = xls.get_tickers()

        for ticker in tickers:
            folder = folder_pattern.replace("<ticker>", ticker)
            if os.path.exists(folder):
                self.migrate(folder, type, ticker, file_pattern)

    def migrate(self, old_folder, type, ticker, file_pattern = None):

        destination = self.get_folder(ticker, type)

        if file_pattern is not None:
            wanted = lambda name: os.path.isfile(os.path.join(old_folder, name)) and file_pattern in name
            move_files = [file for file in os.listdir(old_folder) if wanted(file)]
            for file in move_files:
                self.migrate_file(old_folder, destination, file)
        else:
            destination_parent = os.path.dirname(destination)
            old_folder_name = os.path.basename(old_folder)
            destination_folder_name = os.path.basename(destination)
            if os.path.dirname(old_folder) != destination_parent:
                self.check_directory(destination)
                shutil.move(old_folder, destination_parent)
            if old_folder_name != destination_folder_name:
                os.rename(os.path.join(destination_parent, old_folder_name), destination)

    def migrate_file(self, old_folder, destination, filename):
        dest_file = os.path.join(destination, filename)
        self.check_directory(dest_file)
        shutil.move(os.path.join(old_folder, filename), dest_file)

    def get_instruments(self, name, excluded_tickers = None):
        market_sources = {
            "ASX" : 'market', 
            "NYSE" : 'nyse', 
            "NYSE VALUE" : 'nyse_value'
            }
        exchange = name.upper().split()[0]
        original_exchange = self.exchange
        self.exchange = exchange
        source = market_sources[name.upper()]
        instruments = Instruments(source)
        instruments = self.load(instruments)
        if excluded_tickers is not None:
            instruments.exclude(excluded_tickers)
        self.exchange = original_exchange
        return instruments

    def get_valuations(self, date = None):
        if date is None:
            # Find the most recent valuations
            files = os.listdir(self.valuations)
            filename = Valuations().filename()
            date = self.find_latest_date(files, filename)
        valuations = Valuations(date)
        return self.load(valuations)

    def get_valuemetrics(self, date = None):
        if date is None:
            # Find the most recent valuations
            files = os.listdir(self.valuations)
            filename = ValuationMetrics().filename()
            date = self.find_latest_date(files, filename)
        valuations = ValuationMetrics(date)
        return self.load(valuations)

    def find_latest_date(self, files, filename):
        """
        From a list of files (e.g. retrieved from a directory), and the
        filename format of a file type find the latest dated file of that 
        type. Relies on the date format being of form YYYYMMDD.
        Used for data formats which include a date in the filename, e.g.
        Valuations, ValuationMetrics.
        """
        label = filename[:filename.find("*")]
        suffix = filename[(filename.find("*") + 1):]
        valuation_files = [file for file in files if label in file]
        datenums = [int(file.replace(label, '').replace(suffix, '')) for file in valuation_files]
        latest_date = max(datenums)
        return str(latest_date)



#import os
#os.chdir(os.path.join("C:\\Users", os.getlogin(), "Source\\Repos\\FinancialDataHandling\\financial_data_handling"))

import pandas
pandas.options.display.max_columns = 80

import numpy as np

from formats.price_history import Instruments
from formats.information import ListedCompanies
from formats.fundamentals import Financials
from download.financials import WebDownloader
from store.file_system import Storage
from store.db_wrapper import DbInterface, test_conn_string, build_database, create_engine


print("Ready...")



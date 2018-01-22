
import pandas

from formats import StorageResource
from store.db_wrapper import Company


# TODO Data formats shouldn't be responsible for interpreting downloaded data (e.g. varying table columns).
class ListedCompanies(StorageResource):
    
    def __init__(self, exchange):
        self.exchange = exchange
        self.table = None
        self.headings = pandas.DataFrame(index = ["ticker", "name", "sector", "industry"], columns = ["ASX", "NYSE"], dtype = str)
        self.headings["ASX"] =  ["ASX code", "Company name", "",        "GICS industry group"]
        self.headings["NYSE"] = ["Symbol",   "Name",         "Sector",  "Industry"]
        

    @property
    def index_heading(self):
        return self.headings[self.exchange]["ticker"]

    @property
    def name_heading(self):
        return self.headings[self.exchange]["name"]

    @property
    def sector_heading(self):
        return self.headings[self.exchange]["sector"]

    @property
    def industry_heading(self):
        return self.headings[self.exchange]["industry"]

    @property
    def tickers(self):
        return self.table.index.tolist()

    def select_folder(self, store):
        return store.exchange_information(self)

    def filename(self):
        return self.exchange + "ListedCompanies.xlsx"

    def load_from(self, file_path):
        table = pandas.read_excel(file_path, header = 0)
        table.index = table.pop(self.index_heading)
        self.table = table

    def save_to(self, file_path):
        self.table.to_excel(file_path, sheet_name = "Stock table")

    def get_header(self):
        return self.table.columns.tolist()

    def update_table(self, new_data):
        new_table = pandas.DataFrame.from_dict(new_data, orient = "index")
        self.table = self.table.join(new_table)

    def as_record_list(self):
        record_list = []
        for ticker in self.tickers:
            record_list.append(self.company_record(ticker))
        return record_list

    def company_record(self, ticker):
        name = self.table.loc[ticker, self.name_heading]
        if self.sector_heading:
            sector = self.table.loc[ticker, self.sector_heading]
        else:
            sector = None
        industry = self.table.loc[ticker, self.industry_heading]
        return Company(ticker = ticker, exchange = self.exchange, name = name, sector = sector, industry_group = industry)



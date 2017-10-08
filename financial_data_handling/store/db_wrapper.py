from sqlalchemy import Column, ForeignKey, Integer, Float, String, Boolean, Date, create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base
from datetime import date
from pandas import DataFrame

Base = declarative_base()
db_session = sessionmaker()


class Exchange(Base):
    __tablename__ = 'exchange'

    symbol = Column(String(10), primary_key = True)
    name = Column(String(250))
    country = Column(String(100), nullable = False)
    currency = Column(String(3), nullable = False)
    
class Company(Base):
    __tablename__ = 'company'

    ticker = Column(String(10), primary_key = True)
    exchange = Column(String(10), ForeignKey('exchange.symbol'), primary_key = True)
    name = Column(String(250), nullable = False)
    sector = Column(String(50))
    industry_group = Column(String(50))
    
class Statement(Base):
    '''
    Statements records are the statements which can be generated for a company.
    Line items appearing on a statement are linked through the StatementItem table.
    '''
    __tablename__ = 'statement'
    
    type = Column(String(50), primary_key = True)
    line_items = relationship("LineItem", secondary = "statement_item")

class LineItem(Base):
    '''
    LineItems specify entries which may exist in a statement.
    For example, Revenue, EBIT.
    The income field specifies whether the item is ingoing or outgoing: True (income)
    or negative: False (expense).
    The cumulative field specifies whether the item accumulates over successive
    reporting periods. E.g. revenue is cumulative, whereas assets are not. This
    is used when converting from half yearly to annual values.
    '''
    __tablename__ = 'line_item'

    id = Column(Integer, primary_key = True)
    name = Column(String(250), nullable = False)
    income = Column(Boolean)
    cumulative = Column(Boolean)
    companies = relationship("Company", secondary = "statement_fact")


class StatementItem(Base):
    '''
    StatementItems join LineItems and Statements
    '''
    __tablename__ = 'statement_item'
    
    statement_type = Column(Integer, ForeignKey('statement.type'), primary_key = True)
    line_item_id = Column(Integer, ForeignKey('line_item.id'), primary_key = True)
    row_num = Column(Integer, nullable = False)
    statement = relationship(Statement, backref = backref("line_item_assoc"))
    line_item = relationship(LineItem, backref = backref("statement_assoc"))


class StatementFact(Base):
    '''
    StatementFacts record actual results for a given company and time.
    '''
    __tablename__ = 'statement_fact'

    ticker = Column(String(10), ForeignKey('company.ticker'), primary_key = True)
    line_item_id = Column(Integer, ForeignKey('line_item.id'), primary_key = True)
    date = Column(Date, nullable = False, primary_key = True)
    value = Column(Float)
    company = relationship(Company, backref = backref("line_item_assoc"))
    line_item = relationship(LineItem, backref = backref("company_assoc"))



class DbInterface:

    def __init__(self, db_source):
        if isinstance(db_source, Engine):
            db_session.configure(bind = db_source)
        elif isinstance(db_source, str):
            db_session.configure(bind = create_engine(db_source))
        else:
            raise TypeError("db_source must be an sqlalchemy engine instance or connection string.")
        self.session = db_session()

    def getExchange(self, exchange):
        try:
            exchange = self.session.query(Exchange).filter(Exchange.symbol == exchange).one()
        except NoResultFound as e:
            raise ValueError(ticker + " does not exist")
        return exchange

    def getListedCompanies(self, exchange):
        companies = self.session.query(Company).join(Exchange).filter(Exchange.symbol == exchange).all()
        return companies
    
    def getCompany(self, ticker):
        try:
            company = self.session.query(Company).filter(Company.ticker == ticker).one()
        except NoResultFound as e:
            raise ValueError(ticker + " does not exist")
        return company

    def addCompanies(self, listed_companies):
        current_companies = self.getListedCompanies(listed_companies.exchange)
        additional_companies = listed_companies.as_record_list()
        additional_tickers = [company.ticker for company in additional_companies]
        for existing in current_companies:
            if existing.ticker in additional_tickers:
                additional_companies.pop(additional_tickers.index(existing.ticker))
        self.session.add_all(additional_companies)
        self.session.commit()

    def getLineItem(self, type):
        try:
            line = self.session.query(LineItem).filter(LineItem.name == type).one()
        except NoResultFound as e:
            raise ValueError(type + " does not exist")
        return line

    def addStatementFact(self, ticker, type, date, value):
        company = self.getCompany(ticker)
        line = self.getLineItem(type)
        self.session.add(StatementFact(company = company, line_item = line, date = date, value = value))
        self.session.commit()
        
    def getStatement(self, statement_type, ticker):
        result = self.session.query(StatementItem.row_num, StatementFact.date, LineItem.name, LineItem.cumulative, StatementFact.value).filter(
        StatementFact.line_item_id == LineItem.id).filter(
        LineItem.id == StatementItem.line_item_id).filter(
        StatementItem.statement.has(Statement.type == statement_type)).filter(
        StatementFact.company.has(Company.ticker == ticker)).all()
        df = DataFrame(result)
        df.sort_values(by = 'row_num')
        return df.pivot(index = 'date', columns = 'name', values = 'value')


def build_database(engine):
    Base.metadata.bind = engine
    Base.metadata.create_all()
    db_session.configure(bind = engine)
    session = db_session()

    session.add_all([
        Exchange(symbol = "NYSE", name = "New York Stock Exchange", country = "U.S.", currency = "USD"), 
        Exchange(symbol = "ASX", name = "Australian Stock Exchange", country = "Australia", currency = "AUD")])
    session.commit()


test_conn_string = "sqlite:///D:\\Investing\\Data\\test.db"
    
def buildTestDB(engine):
        Base.metadata.bind = engine
        Base.metadata.create_all()

        db_session.configure(bind = engine)
        session = db_session()
        
        mld = Company(ticker = "MLD", name = "MACA Ltd")
        ccp = Company(ticker = "CCP", name = "Credit Corp")
        session.add(mld)
        session.add(ccp)

        income = Statement(type = "Income")
        balance =  Statement(type = "Balance")
        session.add(income)
        session.add(balance)
        
        revenue = LineItem(name = "Revenue", income = True)
        session.add(revenue)
        expenses = LineItem(name = "Expenses", income = False)
        session.add(expenses)
        session.add(StatementItem(statement = income, line_item = revenue, row_num = 1))
        session.add(StatementItem(statement = income, line_item = expenses, row_num = 2))
        
        assets = LineItem(name = "Assets")
        session.add(assets)
        liab = LineItem(name = "Liabilities")
        session.add(liab)
        session.add(StatementItem(statement = balance, line_item = assets, row_num = 1))
        session.add(StatementItem(statement = balance, line_item = liab, row_num = 2))

        session.add(StatementFact(company = mld, line_item = revenue, date = date(2017, 2, 25), value = 100))
        session.add(StatementFact(company = mld, line_item = expenses, date = date(2017, 2, 25), value = 75))
        session.add(StatementFact(company = mld, line_item = assets, date = date(2017, 2, 25), value = 5000))
        session.add(StatementFact(company = mld, line_item = liab, date = date(2017, 2, 25), value = 2300))

        session.add(StatementFact(company = ccp, line_item = revenue, date = date(2017, 2, 25), value = 80))
        session.add(StatementFact(company = ccp, line_item = expenses, date = date(2017, 2, 25), value = 30))
        session.add(StatementFact(company = ccp, line_item = assets, date = date(2017, 2, 25), value = 5400))
        session.add(StatementFact(company = ccp, line_item = liab, date = date(2017, 2, 25), value = 4300))

        session.commit()
        
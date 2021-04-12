from src.iz_parser import IzParser
import logging
import sqlite3

logger = logging.getLogger(__name__)

con = sqlite3.connect('data/izvestia_inflation.db')

with con:
    con.execute("""create table if not exists News
                   (id integer primary key, date text, title text, url text, content text)
                """)

parser = IzParser('infliatciia', con)
parser.start_parsing(17)
import logging
import sqlite3
from time import sleep
from typing import List

import requests
from bs4 import BeautifulSoup as bs
from dateparser.date import DateDataParser

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('parser.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


class IzParser:
    """Izvestia news parser
    """
    def __init__(self, tag: str, db_connection: sqlite3.Connection) -> None:
        """Init parser

        Args:
            tag (str): desired topic
            db_connection (sqlite3.Connection): sqlite connection
        """
        self.__base_url = 'https://iz.ru'
        self.__con = db_connection
        self.date_parser = DateDataParser(languages=['ru'])
        self.desired_topic = tag

    @property
    def feed_url(self):
        return self.__base_url + '/tag/' + self.desired_topic + '?page='

    def commit_to_db(self, data: List[dict]) -> None:
        """Commit parsed data to sqlite db

        Args:
            data (List[dict]): data to commit
        """        
        with self.__con:
            for article in data:
                self.__con.execute(
                    """
                        insert into News (date, title, url, content)
                        values(:title, :date, :url, :text)
                    """, article)

    def start_parsing(self, num_page: int) -> None:
        """Main parsing loop

        Args:
            num_page (int): number of feed pages to parse
        """
        for i in range(num_page):
            feed_url = self.feed_url + str(i)
            feed_data = self.parse_articles_feed(feed_url)
            self.commit_to_db(feed_data)
            logger.info(f'Page {i} committed successfully')
            sleep(2)

    def parse_articles_feed(self, url: str) -> List[dict]:
        """Parse feed of news without text

        Args:
            url (str): feed url

        Returns:
            List[dict]: [{ 'title': _, 'date': _, 'url': _, 'text': _}, ...]
        """
        response_soup = self.get_response(url, important=True)
        articles_block_soups = response_soup.find_all(
            'div', {'class': 'lenta_news__day'})
        articles_feed = []
        for article_soup in articles_block_soups:
            article_date = self.date_parser.get_date_data(
                article_soup.find('h3').text)
            article_url = self.__base_url + article_soup.find('a').get('href')
            article_title = article_soup.find(
                'div', {'class': 'lenta_news__day__list__item__title'}).text
            article_title = article_title.replace('\n', '').strip()
            article_text = self.parse_article_text(article_url)
            articles_feed.append(
                {'title': article_title, 'date': str(article_date.date_obj), 'url': article_url, 'text': article_text})

        logger.info(f'Feed with url {url} was successfully parsed')
        return articles_feed

    def parse_article_text(self, url: str) -> str:
        """Get text of an article

        Args:
            url (str): article url

        Returns:
            str: article text
        """
        article_soup = self.get_response(url)
        article_texts_soup = article_soup.find(
            'div', {'itemprop': 'articleBody'})
        if article_texts_soup is None:
            article_texts_soup = article_soup.find(
                'div', {'class': 'text-article__inside'})
        article_texts_soup = article_texts_soup.find_all('p')
        text = ''.join([paragraph.text.replace('\n', '').strip()
                       for paragraph in article_texts_soup])
        return text

    @staticmethod
    def get_response(url: str, important: bool = False) -> bs:
        """Tries to request provided url

        Args:
            url (str): website url
            important (bool, optional): rise exception if bad request. Defaults to False.

        Raises:
            Exception: Could not parse important page

        Returns:
            str: page text if 200 response, empty string otherwise
        """
        response = requests.get(url)
        try:
            assert response.status_code == 200, f'Could not parse provided url: {url}'
            return bs(response.text, features='lxml')
        except AssertionError as e:
            logger.error(str(e))
            if important:
                logger.error('Could not parse important page')
                raise Exception('Could not parse important page')
            return bs('', parser='lxml')

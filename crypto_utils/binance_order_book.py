'''Module that handles the extraction and processing of order book data.

Example usage for multiple coins:

symbols = ['LUNAUSDT','ETHUSDT']
order_books = OrderBookComposite()
for symbol in symbols:
    order_books.add_order_book(BinanceOrderBook(symbol))

data = order_books.get_order_book_snapshot()
print(data)


'''

import requests
import logging
import pandas as pd
import numpy as np
import pytest
from abc import ABC, abstractmethod

# setting up the logging framework

try:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    log_file_path = 'logs/binance_order_book.log'
    file_handler = logging.FileHandler(log_file_path)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

except Exception:
    logger = None

class OrderBookBase(ABC):

    @abstractmethod
    def get_order_book_snapshot():
        pass

class BinanceOrderBook(OrderBookBase):
    end_point = 'https://api.binance.com/api/v3/depth'
    req_limit = '5000'

    def __init__(self,symbol):
        if not logger is None:
            logger.debug(f'Instantiating class for {symbol}')
        self.symbol = symbol
        self.price = self.__get_current_price()
        self.levels = self.__get_levels()
        

    def __get_current_price(self):
        url = f'https://api.binance.com/api/v3/ticker/price?symbol={self.symbol}'
        if not logger is None:
            logger.debug(f'Price request is {url}')
        data = requests.get(url).json()
        if not logger is None:
            logger.debug(f'Price data from binance is {data}')

        price = float(data['price'])
        
        return price

    def __get_levels(self):
        '''Gets the price levels corresponding to some percentage
        deviations from current price
        
        Returns:
            dict: represents the bid and ask levels
            '''
        num_levels = [0.001,0.0025,0.005,0.01,0.015,0.020,0.025,
            0.03,0.035,0.04,0.045,0.05,0.06,0.07,
            0.08,0.09,0.10,0.125,0.15,0.2,0.25,0.3,
            0.4,0.5,0.6,0.7,0.8,0.9]

        count = len(num_levels)

        levels = {'bids':[None]*count,'asks':[None]*count}
        for i,num in enumerate(num_levels):
            levels['bids'][i] = (num,self.price*(1-num))
            levels['asks'][i] = (num,self.price*(1+num))

        if not logger is None:
            logger.debug(f'Levels for {self.symbol} are {levels}')

        return levels

    def __get_raw_data(self):
        '''Gets raw data from binance api
        Format returned by API looks like this:

        {'lastUpdateId':int,'bids':[['price','quantity']['price2','quantity2'],etc],
                            'asks':[['price','quantity']['price2','quantity2'],etc]}

        Returns:
            dict
        
        '''
        url = self.end_point + f'?symbol={self.symbol}&limit={self.req_limit}'
        response = requests.get(url)
        data = response.json()
        bids = data['bids'] ; asks = data['asks']
        if not logger is None:
            logger.debug(f'Bid raw data for {self.symbol} is {bids[:10]} ---- {bids[-10:]}')
            logger.debug(f'ask raw data for {self.symbol} is {asks[:10]} ---- {asks[-10:]}')

        return data

    @staticmethod
    def get_processed_data(data,levels,price,type_='bids'):
        '''Takes the raw json from binance and:
            - Transforms strings to floats.
            - Calculate the volume per level
            - Returns a dictionary with volumes
            
        Args:
            data: dict
                eg: {'bids':[['100.05','56'],['101','87']]}
            levels: dict
                eg: {'bids':[[0.02,100],[0.03,105]]}
            type_: str, optional
                Either 'bids' or 'asks'
        Returns:
            dict
            '''

        #first the string transformation
        _data = np.array(data[type_],dtype='float')

        levels = levels[type_]
        result = {}
        series = pd.Series(data=_data[:,1],index=_data[:,0])
        
        for i in range(len(levels)):
            if i == 0:
                prev = price
            else:
                prev = levels[i-1][1]

            threshold = levels[i][1]
            label = levels[i][0]
            if type_=='bids':
                conditions = (series.index<=prev) &\
                            (series.index>threshold)
            elif type_=='asks':
                conditions = (series.index>=prev) &\
                            (series.index<threshold)
            result[label] = series.where(conditions).sum()

        return {type_:result}

    def get_order_book_snapshot(self):
        '''Gets the raw data and processes the bids and the asks
        according to the desired deviation levels.
        
        Returns:
            dict
        '''
        data = self.__get_raw_data()
        bids_snapshot = self.get_processed_data(data,self.levels,
                                                self.price,type_='bids')
        if not logger is None:
            logger.debug(f'Data processed for {self.symbol}, bids. Results:\n {bids_snapshot}')

        asks_snapshot = self.get_processed_data(data,self.levels,
                                                self.price,type_='asks')
        if not logger is None:
            logger.debug(f'Data processed for {self.symbol}, asks. Results:\n {asks_snapshot}')
        snapshot = {**bids_snapshot,**asks_snapshot}
        return snapshot

class OrderBookComposite(OrderBookBase,list):
    def __init__(self):
        super().__init__()
        
    def add_order_book(self,order_book):
        self.append(order_book)

    def get_order_book_snapshot(self):
        result = {}
        for obj in self:
            result[obj.symbol] = obj.get_order_book_snapshot()
            result[obj.symbol].update({'price':obj.price})
        return result






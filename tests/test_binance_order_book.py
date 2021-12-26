import os
import sys
import numpy as np
import pandas as pd
import pytest

from crypto_utils.binance_order_book import BinanceOrderBook

class TestBinanceOrderBook(object):

    def test_get_processed_data_bids(self):
        # test_obj = BinanceOrderBook(symbol='ETHUSDT')
        data = {'bids':[["100","1"],
                ["99","1"],
                ["98","2"],
                ["97.5","1.5"],
                ["96","1"]]}
        levels = {'bids':[(0.02,98),(0.05,95)]}

        expected = {0.02:2,0.05:4.5}
        actual = BinanceOrderBook.get_processed_data(data,levels,100,type_='bids')
        print(actual)
        assert expected == pytest.approx(actual['bids'])

    def test_get_processed_data_asks(self):
        # test_obj = BinanceOrderBook(symbol='ETHUSDT')
        data = {'asks':[["100","1"],
                ["101","1"],
                ["102","2"],
                ["103.5","1.5"],
                ["104","1"]]}
        levels = {'asks':[(0.02,102),(0.05,105)]}

        expected = {0.02:2,0.05:4.5}
        actual = BinanceOrderBook.get_processed_data(data,levels,100,type_='asks')
        print(actual)
        assert expected == pytest.approx(actual['asks'])
    

    def test_get_processed_bids_one_level(self):
        bids = np.array([[100,0.5],[99.8,1],[98,3]])
        series = pd.Series(data=bids[:,1],index=bids[:,0])
        threshold = (100,99)
        conditions = (series.index<=threshold[0]) &\
                    (series.index>threshold[1])
        result = series.where(conditions).sum()
        expected = 1.5
        assert result == pytest.approx(expected)

    def test_get_processed_bids_two_levels(self):
        bids = np.array([[100,0.5],[99.8,1],[98,3],[96,5]])
        series = pd.Series(data=bids[:,1],index=bids[:,0])
        levels = [0.01,0.05]  #percentage
        price = 100
        thresholds = [(100,99),(99,95)]
        result = [None]*len(levels)
        for i,threshold in enumerate(thresholds):
            conditions = (series.index<=threshold[0]) &\
                        (series.index>threshold[1])
            result[i] = series.where(conditions).sum()
        
        expected = [1.5,8]
        assert result == pytest.approx(expected)

    def test_get_processed_bids_all(self):
        data = [[100,1],
                [99,1],
                [98,2],
                [97.5,1.5],
                [96,1]]

        bids = np.array(data)
        series = pd.Series(data=bids[:,1],index=bids[:,0])

        price= 100       
        levels = [(0.002,98),
                (0.004,96),
                (0.006,94)]
        result = [None]*len(levels)
        
        for i in range(len(levels)):
            if i == 0:
                prev = price
            else:
                prev = levels[i-1][1]
            threshold = levels[i][1]

            conditions = (series.index<=prev) &\
                        (series.index>threshold)
            result[i] = series.where(conditions).sum()
        
        expected = [2,3.5,1]
        assert result == pytest.approx(expected)

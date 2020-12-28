import hashlib
import hmac
import json
import sys
import time
from copy import copy
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import pymysql
from vnpy.event import EventEngine
from datetime import datetime, timedelta
from threading import Lock
from vnpy.trader.database import database_manager
import base64
import uuid

from typing import List, Sequence

from requests import ConnectionError

from vnpy.event import Event
from vnpy.api.rest import Request, RestClient
from vnpy.api.websocket import WebsocketClient
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.constant import (
    Direction,
    Exchange,
    OrderType,
    Product,
    Status,
    Interval
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)

REST_HOST = "http://api.coincap.io"

class CoinCapGateway(BaseGateway):
    dbconn = None
    def __init__(self, event_engine):
        """Constructor"""
        super(CoinCapGateway, self).__init__(event_engine, "COINCAP")

        self.rest_api = CoinCapRestApi(self)

    def connect(self, setting: dict):
        self.rest_api.connect()

        # self.dbconn = pymysql.connect(
        #     host='localhost',
        #     user='root',
        #     passwd='dengdeng',
        #     db='cointrader',
        #     port=3306,
        #     charset='utf8'
        # )

    def query_history(self, req: HistoryRequest):
        """
        Query bar history data.
        """
        return self.rest_api.query_history(req)

    def subscribe(self, req: SubscribeRequest):
        """"""
        pass

    def send_order(self, req: OrderRequest):
        """"""
        pass

    def cancel_order(self, req: CancelRequest):
        """"""
        self.rest_api.cancel_order(req)

    def query_account(self):
        """"""
        pass

    def query_position(self):
        """"""
        pass

    def close(self):
        """"""
        self.rest_api.stop()

    def getExchanges(self):
        return self.rest_api.getExchanges()

    def getAssets(self):
        return self.rest_api.getAssets()

    def getCandles(self, req: HistoryRequest):
        return self.rest_api.getCandles(req)

    def saveToDb(self):
        pass



class CoinCapRestApi(RestClient):

    def __init__(self, gateway: BaseGateway):
        """"""
        super(CoinCapRestApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.key = ""
        self.secret = ""
        self.passphrase = ""

        self.order_count = 10000
        self.order_count_lock = Lock()

        self.connect_time = 0

    def query_history(self, req: HistoryRequest):

        path = f"/v2/candles"


        # Create query params
        params = {
            "exchange": "huobi",
            "interval": "d1",
            "baseId": req.symbol,
            "quoteId": "tether",
        }


        # Get response from server
        resp = self.request(
            "GET",
            path,
            params=params
        )
        #data: List[BarData] = []
        data: List[BarData] = []
        dataList = resp.json()["data"]
        for item  in dataList:
            o = item["open"]
            h = item["high"]
            l = item["low"]
            c = item["close"]
            v = item["volume"]
            dt = item["period"]
            #dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(dt))
            # u = 1439111214.0  # unix时间戳
            dt = datetime.fromtimestamp(dt/1000)
            bar = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=dt,
                interval=req.interval,
                volume=float(v),
                open_price=float(o),
                high_price=float(h),
                low_price=float(l),
                close_price=float(c),
                gateway_name=self.gateway_name
            )
            data.append(bar)
        #print(data)
        return data

    def connect(self):
        self.connect_time = int(datetime.now().strftime("%y%m%d%H%M%S"))

        self.init(REST_HOST)
        self.start()
        self.gateway.write_log("REST API启动成功")

    def getExchanges(self):
        path = f"/v2/exchanges"

        resp = self.request(
            "GET",
            path
        )
        return resp.json()["data"]

    def getAssets(self):
        path = f"/v2/assets"
        resp = self.request(
            "GET",
            path
        )
        return resp.json()["data"]

    def getCandles(self,req: HistoryRequest):
        path = f"/v2/candles"

        params = {
            "exchange": req.exchange,
            "interval": req.interval,
            "baseId": req.baseId,
            "quoteId": req.quoteId
        }

        resp = self.request(
            "GET",
            path,
            params=params
        )

        data = resp.json()["data"]

        return data


def dataFrametoDB(dataFrame,tableName,if_exists = 'replace'):
    engine = create_engine('mysql+mysqlconnector://root:foresee@localhost:3306/cointrader')
    pd.io.sql.to_sql(dataFrame, tableName, con=engine, index=False, if_exists=if_exists)
def main():
    event_engine = EventEngine()
    coinCapGateway = CoinCapGateway(event_engine)
    coinCapGateway.connect(None)
        # req = HistoryRequest("eos",Exchange.HUOBI,start="1111",end=None,interval=Interval.DAILY)
        # data = coinCapGateway.query_history(req)
        # if data:
        #     database_manager.save_bar_data(data)

    #data = coinCapGateway.getExchanges()
    symbol = "BTC/USDT"
    req = HistoryRequest(symbol,Exchange.HUOBI,start="1111")
    req.exchange = "huobi"
    req.interval = "m1"
    req.baseId = "bitcoin"
    req.quoteId = "tether"
    data = coinCapGateway.getCandles(req)


    df = pd.DataFrame(data)
    df["exchange"] = req.exchange
    df["symbol"] = symbol
    df["interval"] = req.interval

    # dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(dt))
    # u = 1439111214.0  # unix时间戳
    # dt = datetime.fromtimestamp(dt / 1000)
    #
    # df["period2"] = datetime.fromtimestamp(df["period"] / 1000)
    #
    # #print(data)
    # # df= pd.read_json(data, encoding="utf-8", orient='records')
    # # print(df.head())
    #
    # df.to_csv("exchanges.csv")

    # engine = create_engine(
    #     'mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)d/%(database)s?charset=utf8' % dbconn, encoding='utf-8')
    # df = pd.DataFrame(np.arange(20).reshape(5, 4), index=[1, 3, 4, 6, 8])
    #print(df)
    #df.to_sql(name='exchanges', con=dbconn, if_exists='replace',index=False)
    dataFrametoDB(df, "candles","append")
    # api.coincap.io/v2/markets?exchangeId=huobi
    #df.to_sql(name='exchanges', con=dbconn, if_exists='replace',index=False)


if __name__ == "__main__":
    main()
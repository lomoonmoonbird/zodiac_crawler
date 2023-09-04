# --*-- coding: utf-8 --*--
"""
setting base
1: mysql setting base
"""
from types import SimpleNamespace

class MysqlSettings(dict):
    def __init__(self):
        self.host = '192.168.5.140'
        self.user = 'root'
        self.pwd = 'PengKim@123'
        self.port = 3306
        self.charset = 'utf8'
        self.db = 'dorado'



class SettingsBase(dict):
    def __init__(self):
        self.mysqldb = SimpleNamespace(**{
            "crawl": MysqlSettings()
        })

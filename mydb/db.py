# --*-- coding: utf-8 --*--

"""
mysql database manager
"""
# from __future__ import absolute_import
import pymysql
from pymysql import err
from settings.current_settings import get_current_setting

class DB(object):
    def __init__(self):
        self.setting = get_current_setting().mysqldb.crawl
        self.connection = self.connection()

    def connection(self):
        connection=pymysql.connect(host=self.setting.host,
                                   user=self.setting.user,
                                   password=self.setting.pwd,
                                   db=self.setting.db,
                                   port=self.setting.port,
                                   autocommit=False,
                                   charset=self.setting.charset)
        return connection


    def create_table(self, table_name, sql):
        cursor = self.connection.cursor()
        # drop table using execute command
        cursor.execute("DROP TABLE IF EXISTS " + table_name)
        #create table using execute command
        try:
            ret = cursor.execute(sql)
        except err.InternalError as e:
            print (e)
        finally:
            pass


    def batch_insert(self, sql_data):
        cursor = self.connection.cursor()
        try:
            for s_d in sql_data:
                cursor.execute(s_d[0], s_d[1])
            self.connection.commit()
        except err.InternalError as e:
            print (e,'@@@@@')
        finally:
            # pass
            # self.connection.commit()
            self.connection.close()

    def insert(self, sql, data):
        cursor = self.connection.cursor()
        cursor.execute(sql, data)

    def update(self, sql):
        cursor = self.connection.cursor()
        cursor.execute(sql)

    def find(self,sql):
        cursor = self.connection.cursor()
        ret = cursor.execute('select * from `aaa`')
        result = cursor.fetchone()
        print (result)

    def close(self, connection):
        connection.close()
#
# db =DB()
# # # db.create_table('aaa')
# sql = 'INSERT INTO `aaa` (`name`, `dateofbirthday`, `strength`, `weakness`, `symbol`, `element`, ' \
#       '`signruler`,`luckycolor`, `luckynumber`,`jewelry`,`bestmatch`,`celebrities`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
# db.batch_insert([(sql, ("aaa1111", "bbb", "cccc", "ddd", "dda", "eee", "fff", "ddd",2,"sadasd","dsads","ddddd")),
#                  (sql, ("aaa1111", "bbb", "cccc", "ddd", "dda", "eee", "fff", "ddd",2,"sadasd","dsads","ddddd")),
#                  (sql, ("aaa1111", "bbb", "cccc", "ddd", "dda", "eee", "fff", "ddd",2,"sadasd","dsads","ddddd"))])
# cursor = db.connection.cursor()
# ret =  cursor.execute(sql)
# print (ret)
# db.find('select * from `aaa`')
# cursor.execute(sql, ("aaa1111", "bbb", "cccc", "ddd", "dda", "eee", "fff", "ddd",2,"sadasd","dsads","ddddd"))
# db.connection.commit()

# a = [
#         "INSERT INTO `aaa` (`name`, `dateofbirthday`, `strength`, `weakness`, `symbol`, `element`, `signruler`,`luckycolor`, `luckynumber`,`jewelry`,`bestmatch`,`celebrities`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
#     (
#             "taurus",
#             "april20-may20",
#             "romantic,decisive,logical,diligent,ardent,patient,talentedinart,perseverant,benevolent",
#             "prejudiced,dependent,stubborn",
#             "bull",
#             "earth",
#             "venus",
#             "pink",
#             6,
#             "emerald,jade",
#             "capricorn,virgoandtaurus",
#             "karlmarx,williamshakespeare,leonardodavinci,davidbeckham,alpacino"
#     )
#     ]
#
#
# db.batch_insert(a)

# print ( "INSERT INTO `aaa` (`name`, `dateofbirthday`, `strength`, `weakness`, " \
# "`symbol`, `element`, `signruler`,`luckycolor`, `luckynumber`,`jewelry`," \
# "`bestmatch`,`celebrities`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (
#   1,2,3,4,5,6,7,8,9,10,11,12
# ) )
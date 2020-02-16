# -*- coding:utf-8 -*-
"""
Python 连接 Mysql数据库工具类
"""
import time
import pymysql


# 获取当前时间
def getCurrentTime():
    return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))


class utils:

    # 数据库初始化
    def _init_(self, host, user, passwd, db, port=3306, charset='utf8'):
        pymysql.install_as_MySQLdb()
        try:
            self.db = pymysql.connect(host=host, user=user, passwd=passwd, db=db, port=3306, charset=charset)
            # self.db = pymysql.connect(ip, username, pwd, schema,port)
            self.db.ping(True)  # 使用mysql ping来检查连接,实现超时自动重新连接
            print(getCurrentTime(), u"MySQL DB Connect Success:", user + '@' + host + ':' + str(port) + '/' + db)
            self.cur = self.db.cursor()
        except  Exception as e:
            print(getCurrentTime(), 'ERROR: ', u"MySQL DB Connect Error :%d: %s" % (e.args[0], e.args[1]))

    # 插入数据
    def insertData(self, table, my_dict):
        try:
            # self.db.set_character_set('utf8')
            cols = ', '.join(my_dict.keys())
            values = '"," '.join(my_dict.values())
            sql = "replace into %s (%s) values (%s)" % (table, cols, '"' + values + '"')
            # print (sql)
            try:
                result = self.cur.execute(sql)
                insert_id = self.db.insert_id()
                self.db.commit()
                # 判断是否执行成功
                if result:
                    # print (self.getCurrentTime(), u"Data Insert Sucess")
                    return insert_id
                else:
                    return 0
            except Exception as e:
                # 发生错误时回滚
                self.db.rollback()
                print(getCurrentTime(), 'ERROR: ', u"Data Insert Failed: %s" % (e))
                return 0
        except Exception as e:
            print(getCurrentTime(), 'ERROR: ', u"MySQLdb Error: %s" % (e))
            return 0


    def select(self, sql):
        try:
            try:
                a = self.cur.execute(sql)
                return a
            except Exception as e:
                # 发生错误时回滚
                self.db.rollback()
                print(getCurrentTime(), 'ERROR: ', u"Data Insert Failed: %s" % (e))
                return 0
        except Exception as e:
            print(getCurrentTime(), 'ERROR: ', u"MySQLdb Error: %s" % (e))
        return 0
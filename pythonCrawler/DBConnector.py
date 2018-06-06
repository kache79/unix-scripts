#! /usr/bin/python
import logging, MySQLdb, re
from variables import variables

class DBConnector():

	def __init__(self):
		self.host = variables[0]
                self.user = variables[1]
                self.password = variables[2]
                self.dataBase = variables[3]
		self.establishConnection()

	def establishConnection(self):
		logging.debug("Connecting to db host: "+self.host)
		self.db = MySQLdb.connect(host=self.host, user=self.user, passwd=self.password, db=self.dataBase)
		self.cur = self.db.cursor()
		self.db.set_character_set('utf8')
	def closeConnection(self):
		self.db.close()
	def commit(self):
		self.db.commit()
	def getCursor(self, sqlString=''):
		if not sqlString == '':
			self.cur.execute(sqlString)
		return self.cur
	def executeQuery(self, sqlQuery):
		self.cur.execute(sqlQuery)
	def insertData(self, sqlQuery, insertData):
		if not self.db.open:
			self.establishConnection()
		try:
			self.cur.execute(sqlQuery, insertData)
			self.db.commit()
		except:
			raise
	def insertMultiple(self, sqlQuery, data):
		if not self.db.open:
                        self.establishConnection()
                try:
                        self.cur.executemany(sqlQuery, data)
                        self.db.commit()
                except:
                        raise

def main():
	logging.debug("DBConnector main function called")

if __name__ == '__main__':
	main()

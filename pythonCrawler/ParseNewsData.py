#! /usr/bin/python
import logging, re, json
from datetime import datetime, timedelta

class ParseNewsData():

	def __init__(self, dataText):
		self.dataText = dataText
		self.jsonData = json.loads(self.dataText)
		self.title = self.jsonData['Title']
		self.body = self.jsonData['Body']
		self.dataDate = self.jsonData['Time']
		self.createDate = self.formatDataDate(self.dataDate)

	def formatDataDate(self, formatDate):
		formatDate = re.findall('\d+', formatDate)
		formatDate = datetime.fromtimestamp(int(formatDate[0]) / 1e3).strftime('%Y-%m-%d %H:%M:%S')
		return formatDate
	def getTitle(self):
		return self.title
	def getBody(self):
		return self.body
	def getTime(self):
		return self.createDate
	def getCategoryFromTitle(self):
		try :
			category = re.search('(.+?)-', self.title).group(1)
		except AttributeError:
			category = 'General'
		category = re.sub(r'\d', '', category)
		if not category.isupper():
			category = 'General'
		return category
	def getAll(self):
		return self.title, self.body, self.createDate

def main():
	logging.debug('ParseNewsData main function')

if __name__ == '__main__':
	main()

#! /usr/bin/python
import logging, bs4, re
from collections import OrderedDict

class ParseURL():

	def __init__(self, responseText):
		self.responseText = responseText
		self.soup = bs4.BeautifulSoup(self.responseText, 'lxml')

	def getLinks(self):
		links = []
		results = self.soup.findAll('li', {'data-page': True})
		for result in results:
                	link = result.findAll('a')[0]
			links.append(link.attrs['href'])
		logging.debug("Links Parsed, returning")
		return list(OrderedDict.fromkeys(links))
	def getNewsId(self):
		ids = []
		results = self.soup.findAll('div', {'class': 'mbArticleText newsTitle'})
		for result in results:
			temp = [result['data-id'], re.sub(' ','%20',result['data-srccode'])]
			ids.append(temp)
		return ids

def main():
	logging.debug("ParserURL main function")

if __name__ == '__main__':
	main()

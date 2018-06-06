#! /usr/bin/python

import requests, sys, logging
from variables import webVariables as variables

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class WebSession():

	def __init__(self):
		self.loginUrl = variables[0]
		self.userName = variables[1]
		self.password = variables[2]
		self.login_data = {'UserName': self.userName, 'Password': self.password,}
		#self.session = requests.session()
		self.session = requests.Session()
		logging.debug("Session started")

	def login(self, postData):
		return self.session.post(self.loginUrl, data=postData)

	def getUrlRequest(self, url):
		logging.debug("Sending GET request to url:" + str(url))
		try:
			return self.session.get(url)
		except:
			print("Error getting data from "+str(url))
			return ''

def main():
	logging.debug("Sessions started")
	web = WebSession()
	web.login(web.login_data)
	web.getUrlRequest("https://lk.olb.ru/Information/News/REUTERS%20(EN)")

	#r = session.get("https://lk.olb.ru/Information/News/REUTERS%20(EN)")
	#tmpFile = open('tempWeb.txt', 'wb')
	#for chunk in r.iter_content(100000):
	#	tmpFile.write(chunk)
	#tmpFile.close()

if __name__ == '__main__': main()

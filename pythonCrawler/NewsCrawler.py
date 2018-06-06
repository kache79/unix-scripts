#! /usr/bin/python
import logging, os, sys, signal
import argparse
import WebSession
import ParseURL
import ParseNewsData
import DBConnector
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Thread, current_thread
from Queue import Queue

#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.disable(logging.CRITICAL)

class NewsCrawler:

	def printNewline(self, string=""):
		if string == "":
			print ""
		else:
			print "["+time.strftime("%Y-%m-%d %H:%M:%S")+"] "+str(string)
	def printRolling(self, string):
		string = "["+time.strftime("%Y-%m-%d %H:%M:%S")+"] "+str(string)
		try:
			rows, columns = os.popen('stty size', 'r').read().split()
		except Exception:
			print string
			return
		print string.ljust(int(columns), ' ')+"\r",
		sys.stdout.flush()
	def printProgress(self, string, current, total):
		string = "("+str(current)+"/"+str(total)+") "+str(string)
		try:
			rows, columns = os.popen('stty size', 'r').read().split()
		except Exception:
			print string
			return
		print string.ljust(int(columns), ' ')+"\r",
		sys.stdout.flush()
	def storeIdFromDbToFile(self):
		idList = list(sum(self.db.getCursor("SELECT newsId FROM vtbNews WHERE id > 0").fetchall(), ()))
		with open('processedIs.list', 'w') as tmpFile:
			for item in idList:
				tmpFile.write(item+'\n')

	def getLastDayFromDB(self):
		data = self.db.getCursor("SELECT DATE_FORMAT(dateFromId, '%d.%m.%Y') as lastDate FROM vtbNews group by dateFromId ORDER BY dateFromId LIMIT 1").fetchall()
		for row in data:
			return str(row[0])

	def checkConsistency(self):
		sqlString = "SELECT COUNT(newsId) from vtbNews where id > 0;"
		#self.printNewline("Getting count from DB ...")
		data = self.db.getCursor(sqlString).fetchall()
		dbCount = 0
		for row in data:
			dbCount = int(row[0])
		#self.printNewline("Count from DB: "+str(dbCount))
		with open('processedIs.list', 'r') as f:
			 for i, l in enumerate(f):
				pass
		i += 1
		self.printNewline("Rows in DB: "+str(dbCount)+" rows in file: "+str(i))
		if dbCount == i:
			return True
		else:
			return False


	def __init__(self, startDate= '', endDate= '', loop=False, proceed=True, fastMode=False, dumpIds=False):
		self.linkThreads = 35
		self.newsThreads = 50
		self.timeoutShort = 5
		self.timeoutLong = 60
		self.fastMode = fastMode
		self.loop = loop
		if self.fastMode:
			self.printNewline("Fast Mode Activated ...")
		self.printNewline("Logging In ...")
                self.web = WebSession.WebSession()
                self.web.login(self.web.login_data)
                self.printNewline("Connecting to DB ...")
                self.db = DBConnector.DBConnector()
		self.printNewline("Connection to DB Established ...")
		self.printNewline("Checking consistency with DB ...")
		if startDate == '' and proceed:
			self.printNewline("Getting last date from DB ...")
			startDate = self.getLastDayFromDB()
		self.firstDate = startDate
		self.lastDate = endDate
		try:
			datetime.strptime(self.firstDate, '%d.%m.%Y')
		except ValueError:
			self.printNewline("No valid start date provided, assigning todays date")
			self.firstDate = time.strftime("%d.%m.%Y")
		try:
			datetime.strptime(self.lastDate, '%d.%m.%Y')
		except ValueError:
			self.printNewline("No valid end date provided, assigning.")
			if loop == True:
				self.lastDate = time.strftime("%d.%m.%Y")
			else:
				self.lastDate = '01.01.2015'
		self.daysArray = self.crawlBackwards(self.firstDate, self.lastDate)
		self.printNewline("Checking consistency with DB ...")
		#self.checkConsistency()
		if dumpIds or not self.checkConsistency():
			self.printNewline("Dumping Existing Id's from DB into file")
			self.storeIdFromDbToFile()
			#exit(0)
	        self.getExistingIds()
		self.printNewline("Loaded existing ID's to memory: "+str(len(self.idList)))
	        #printNewline "Processing links from dates array, total: "+str(len(daysArray))+" days"
		if loop == True:
			self.printNewline("Starting in loop mode with todays date")
			while True:
				#startDate = time.strftime("%d.%m.%Y")
				#endDate = time.strftime("%d.%m.%Y")
				#self.daysArray = self.crawlBackwards(startDate, endDate)
				#self.runCrawlerThreaded()
				self.loopCrawler()
				self.printRolling("Loop finished, sleeping 20 sec")
				time.sleep(20)
		else:
			#self.runCrawler()
			self.runCrawlerThreaded()
		self.db.closeConnection()
		self.printNewline("Done.")

	def getExistingIds(self):
		try:
			with open('processedIs.list', 'r') as tmpFile:
				self.idList = tmpFile.read().splitlines()
		except IOError:
			self.idList = list()
	def appendIdToFile(self, newsId):
		 with open('processedIs.list', 'a') as tmpFile:
			tmpFile.write(newsId)
	def getLinkParser(self, link):
		self.printRolling("Downloading "+link)
		dataText = ""
		try:
                        if self.fastMode:
				timeout = self.timeoutShort
				#data = self.web.session.get("https://lk.olb.ru"+link, timeout=15)
			else:
				timeout = self.timeoutLong
				#data = self.web.session.get("https://lk.olb.ru"+link)
			data = self.web.session.get("https://lk.olb.ru"+link, timeout=timeout)
			dataText = ParseURL.ParseURL(data.text)
                except Exception:
                        return False
                return dataText

	def populateLinksQueue(self, queue, results_queue):
		while not queue.empty():
			link = queue.get_nowait()
			linkParser = self.getLinkParser(link)
			if linkParser != False:
				idArray = linkParser.getNewsId()
				results_queue.put_nowait(idArray)
			queue.task_done()
			sys.stdout.flush()

	def loopCrawler(self):
		self.getExistingIds()
		idArray = []
		currentDay =  time.strftime("%d.%m.%Y")
		link = "https://lk.olb.ru/Information/News/REUTERS%20(EN)/"+currentDay+"/"+currentDay+"/0"
		self.printRolling("Downloading from "+link)
		linkParser = self.getLinkParser(link)
		data = self.web.session.get(link)
                dataText = ParseURL.ParseURL(data.text)
		idArray = dataText.getNewsId()
		self.printRolling("Id's found "+str(len(idArray)))
		self.processNewsArrayThreaded(idArray)
		self.printRolling("Loop finished, waiting ...")
		del data
		del dataText
		del idArray

	def runCrawlerThreaded(self):
		if not self.loop:
			self.printNewline("Processing links from dates array, total: "+str(len(self.daysArray))+" days")
		for currentDay in self.daysArray:
			#self.printNewline ("Processing day "+currentDay+" ...")
			self.printRolling("Processing day "+currentDay+" ...")
			try:
				data = self.web.session.get("https://lk.olb.ru/Information/News/REUTERS%20(EN)/"+currentDay+"/"+currentDay+"/0", timeout=60)
			except Exception:
				self.printNewline("Error getting links for day "+currentDay+" ...skipping ...")
				with open("skipped.days", "a+") as f:
					f.write(currentDay)
				continue
                        #parser = ParseURL.ParseURL(self.web.getUrlRequest("https://lk.olb.ru/Information/News/REUTERS%20(EN)/"+currentDay+"/"+currentDay+"/0").text)
                        parser = ParseURL.ParseURL(data.text)
			links = parser.getLinks()
                        i = 0
                        idArray = []
			queue = Queue()
			results_queue = Queue()
			for link in links:
				queue.put(link)
			numThreads = len(links)
			if numThreads > self.linkThreads:
				numThreads = self.linkThreads
			#self.printRolling("Found "+str(len(links))+" links, running "+str(numThreads)+" threads ...")
			for i in range(numThreads):
				thread = Thread(target=self.populateLinksQueue, args=(queue, results_queue))
				thread.daemon = True
				thread.start()

			queue.join()

			while not results_queue.empty():
				data = results_queue.get_nowait()
				if data != False:
					idArray += data
				#idArray += results_queue.get_nowait()

			self.processNewsArrayThreaded(idArray)
                        #self.printNewline("")
                        del parser
                        del links

	def getNewsById(self, newsId):
                link = "https://lk.olb.ru/Information/NewsById?newsId="+newsId[0]+"&sourceCode="+newsId[1]
                #return self.web.getUrlRequest(link)
		try:
			if self.fastMode:
				timeout = self.timeoutShort
				#data = self.web.session.get(link, timeout=10)
			else:
				timeout = self.timeoutLong
				#data = self.web.session.get(link)
			data = self.web.session.get(link, timeout=timeout)
		except Exception:
			return False
		return data

	def processRun(self, queue, result_queue):
		i = 0
		while not queue.empty():
			i += 1
			id = queue.get_nowait()
			self.printRolling("("+str(i)+") Downloading "+id[0])
			if id != False:
				data = self.getNewsById(id)
				if data != False:
					result_queue.put_nowait((id, data))
			queue.task_done()
			sys.stdout.flush()

	def processNewsArrayThreaded(self, idArray):
		sqlData = []
                appendId = ""
                count = 0
                current = 0
                sqlString = "INSERT INTO vtbNews (newsId, title, body, printDate, dateFromId, filePath, category) VALUES(%s, %s, %s, %s, %s, %s, %s)"
		queue = Queue()
		result_queue = Queue()
		numThreads = 0
		for id in idArray:
			if id[0] not in self.idList:
				queue.put(id)
				numThreads += 1
		logging.debug("Queue populated with "+str(numThreads)+" links, running Threads")
		if numThreads > self.newsThreads:
			numThreads = self.newsThreads
		for i in range(numThreads):
			thread = Thread(target=self.processRun, args=(queue, result_queue))
			thread.daemon = True
			thread.start()

		queue.join()
		logging.debug("Queue finished downloading News")
		while not result_queue.empty():
			count += 1
			id, data = result_queue.get_nowait()
			dataText = data.text
                        newsId = id[0]
			self.printRolling("Parsing "+id[0])
			sqlData.append(self.processNews(id, dataText))
			appendId += str(id[0])+'\n'
			#result_queue.task_done()
                        if count > 200:
				self.printRolling("Inserting 200+ data to DB")
                        	try:
                                	self.db.insertMultiple(sqlString, sqlData)
                                        self.appendIdToFile(appendId)
                                        appendId = ""
                                        sqlData = []
                                        count = 0
				except Exception:
                                	print "Error insert into DB, trying to login, skipping loop"
					self.db.establishConnection()
					result_queue.task_done()
					continue
			result_queue.task_done()
                try:
                        self.db.insertMultiple(sqlString, sqlData)
                except Exception:
                        print("DB insert Error on id: "+str(id[0]))
                        return
                self.appendIdToFile(appendId)

	def processNews(self, id, dataText):
		rawFolder = 'data/jsonData/'
		dataFolder = 'data/newsData/'
		fileName = id[0].split(':')[4]
		filePath = id[0].split(':')[2]+'/'+id[0].split(':')[3]+'/'
		try:
			newsParser = ParseNewsData.ParseNewsData(dataText)
		except Exception:
                	print("newsParser error on id: "+str(fileName))
			return
		title, body, createDate = newsParser.getAll()
		category = newsParser.getCategoryFromTitle()
		self.storeToFile(fileName, dataFolder+filePath, title+'\n'+body)
                del newsParser
		return (id[0], title, body, createDate, id[0].split(':')[3], dataFolder+filePath+fileName, category)

	def crawlBackwards(self, firstDate=time.strftime("%d.%m.%Y"), lastDate='01.01.2015'):
		returnDays = []
		dateObject = datetime.strptime(firstDate, '%d.%m.%Y')
		lastDay =  datetime.strptime(lastDate, '%d.%m.%Y')
		while dateObject >= lastDay:
			returnDays.append(dateObject.strftime('%d.%m.%Y'))
			dateObject = dateObject - timedelta(days=1)
		return returnDays

	def storeToFile(self, fileName, folderPath, data):
		if not os.path.exists(folderPath):
			os.makedirs(folderPath)
		#tmpFile = open(folderPath+fileName, 'wb')
		if isinstance(data, unicode) or  isinstance(data, str):
			tmpFile = open(folderPath+fileName, 'w')
			tmpFile.write(data.encode('utf-8'))
		else:
			tmpFile = open(folderPath+fileName, 'wb')
			try:
				for chunk in data.iter_content(100000):
					tmpFile.write(chunk)
			except AttributeError:
				logging.debug("Error writing "+fileName+". Wrong data type, skipping")
		tmpFile.close()
		del tmpFile

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-s", "--start", help="Start Date", default='')
	parser.add_argument("-e", "--end", help="End date", default='')
	parser.add_argument("-l", "--loop", help="For infinite loop", action="store_true")
	parser.add_argument("-p", "--proceed", help="Proceed from the last date in DB", action="store_true")
	parser.add_argument("-f", "--fast", help="Initiates fast sync mode", action="store_true")
	parser.add_argument("-d", "--dump", help="Dump existing news Id's from DB", action="store_true")

	args = parser.parse_args()

	NewsCrawler(args.start, args.end, args.loop, args.proceed, args.fast, args.dump)


def exit_gracefully(signum, frame):
    # restore the original signal handler as otherwise evil things will happen
    # in raw_input when CTRL+C is pressed, and our signal handler is not re-entrant
    signal.signal(signal.SIGINT, original_sigint)
    sys.exit()
    try:
        if raw_input("\nReally quit? (y/n)> ").lower().startswith('y'):
		sys.exit(0)

    except KeyboardInterrupt:
		print("Ok ok, quitting")
		sys.exit(0)

    # restore the exit gracefully handler here    
    signal.signal(signal.SIGINT, exit_gracefully)




if __name__ == '__main__':
	#original_sigint = signal.getsignal(signal.SIGINT)
	#signal.signal(signal.SIGINT, exit_gracefully)
	#variables = ["192.168.2.3", "kache", "Krishkas1", "dataCrawler"]
	#with open('variables.data', 'w') as f:
	#	f.write('variables = %s' % variables)
	#exit (0)
	main()

#!/usr/bin/env python
import httplib
import json
import logging
import time
import sys
import elasticsearch

# environment initialization
reload(sys)
sys.setdefaultencoding('utf-8')

FUNDA_API_HOSTNAME = 'partnerapi.funda.nl'
FUNDA_API_KEY = '005e7c1d6f6c4f9bacac16760286e3cd'
FUNDA_API_PATH = 'feeds/Aanbod.svc'

averages = []
es = elasticsearch.Elasticsearch()

class FundaMain:
	def __init__(self, config = None):
		if config :
			self.hostname = config['hostname']
			self.key = config['key']
			self.path = config['path']
		else:
			self.hostname = FUNDA_API_HOSTNAME
			self.key = FUNDA_API_KEY
			self.path = FUNDA_API_PATH

		self.makelaars = {}
		self.key_path = '/' + self.path + '/' + self.key

	def add(self, result):
		#print json.dumps(result, indent=2, sort_keys=True)
		#raise Exception
		for o in result["Objects"]:
			es.index(index='funda-current', doc_type='object',id=o['Id'],body=o)

	def start(self, filters):
		headers = {
			'Content-Type':'text/plain; charset=utf-8',
			'Accept' : 'application/json'
		}

		totalPages = -1
		filters['page'] = 1
		filters['pagesize'] = 25
		connection = httplib.HTTPConnection(self.hostname)

		success = True
		while success and (int(filters['page']) < totalPages or totalPages < 0) :

			parameters = '&'.join('%s=%s' % (key,value) for key, value in filters.iteritems())

			try:
				connection.request('GET', self.key_path + '/?' + parameters, None, headers)
				response = connection.getresponse()
				status = response.status

				if status == 200:
					data = response.read()
					result = json.loads(data)
					if totalPages < 0 :
						totalPages = result['Paging']['AantalPaginas']
						totalItems = result['TotaalAantalObjecten']
						print 'There are %s results in %s page%s.' % (totalItems, totalPages, 's' if totalPages>1 else '')

					self.add(result)

					filters['page'] = int(filters['page']) + 1

				else:
					raise HTTPException

			finally:
				connection.close()

		return
		

validInput = False
hasGarden = None

zo = '/amsterdam/'

filters = {'type':'koop', 'zo': zo }

main = FundaMain()
main.start(filters)

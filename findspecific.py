#!/usr/bin/env python
import httplib
import json
import logging
import time
import sys
import numpy

# environment initialization
reload(sys)
sys.setdefaultencoding('utf-8')

FUNDA_API_HOSTNAME = 'partnerapi.funda.nl'
FUNDA_API_KEY = '005e7c1d6f6c4f9bacac16760286e3cd'
FUNDA_API_PATH = 'feeds/Aanbod.svc'

averages = []

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

		#print result

		for o in result["Objects"]:
			oppervlak = o["Woonoppervlakte"]
			prijs = o["Prijs"]["Koopprijs"]
			postcode = o["Postcode"]
			if oppervlak and prijs:
				avg = prijs/oppervlak
				averages.append(avg)
				print "Postcode " + postcode + ": " + str(avg) + " EUR/m2 (" + str(oppervlak) + "m2 E" + str(prijs) + ", " + o["Soort-aanbod"] + ") " + o["URL"] + "  " + o["Adres"]

		print " - Running average after this page is " + str(sum(averages)/len(averages))

		#raise Exception
		#for makelaarNaam in list(map(lambda makelaar : makelaar['MakelaarNaam'], result['Objects'])) :
		#	if makelaarNaam in self.makelaars :
		#		self.makelaars[makelaarNaam] += 1
		#	else :
		#		self.makelaars[makelaarNaam] = 1

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

			success = (int(filters['page']) < 50)

		return #list(results) if results else None


validInput = False
hasGarden = None

zo = '/amsterdam/450000-600000/100-110-woonopp/'

filters = {'type':'koop', 'zo': zo }

main = FundaMain()
main.start(filters)

cnt = len(averages)
print "OVERALL average for " + str(len(averages)) + " objects is " + str(sum(averages)/len(averages))
print "lowest: " + str(min(averages))
print "highest: " + str(max(averages))
print "histogram: "
hist = numpy.histogram(averages, bins = range(1000,10000,1000))
print "    ",
for i in hist[0]:
	print "%s%%" % str(int(float(100*i/(cnt)))).rjust(9),
print
for i in hist[1]:
	print "%s" % str(i).rjust(10),

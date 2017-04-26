import time
import requests
import gzip
import md5
import json

SETTINGS = {
	'benicewait' : 10,
	'timeout' : 1,
	'output_path' : '/live/epa/',
	'urlbase' : 'https://opendata.epa.gov/data/facility/XX',
	'report_url' : 'https://opendata.epa.gov/usepa/app/queries/pollution-reports.rq?results&tqx=out:table&facilityuri=https%3A//opendata.epa.gov/data/facility/XX&_=1493240807623',
	'graph_url' : 'https://opendata.epa.gov/usepa/app/queries/chemicals-mediums-by-facility.rq?results&facilityuri=https%3A//opendata.epa.gov/data/facility/XX&tqx=reqId%3A0'
	'skip_file' = '/live/epa/skip.txt'
}

# Ingest list of facilities
facilities = ['110005239241', '110056975536', ]


if __name__ == '__main__':
		
	for f in facilities:
		print '-> %s' % f

		try:

			url = SETTINGS['urlbase'].replace('XX',f)
			file_html = SETTINGS['output_path'] + f + '-page.html.gz'
			file_json = SETTINGS['output_path'] + f + '.json.gz'
			r = requests.get(url, timeout=SETTINGS['timeout'])
			j = requests.get(url + '.json', timeout=SETTINGS['timeout']) 
			
			# Get HTML version
			with gzip.open(file_html, 'wb') as gzf:
				gzf.write(r.text.encode('ascii','ignore'))
			
			# Get JSON version
			with gzip.open(file_json, 'wb') as gzf:
				gzf.write(j.text.encode('ascii','ignore'))

			# Gather pollution reports
			url = SETTINGS['report_url'].replace('XX', f)
			js = requests.get(url, timeout = SETTINGS['timeout']).text
			file_json_reports = SETTINGS['output_path'] + f + '-reports.json.gz'
			with gzip.open(file_json_reports,'wb') as gzf:
				gzf.write(js.encode('ascii','ignore'))
			
			data = json.loads(js)
			for row in data['rows']:
				url = row[0] + '.json'
				report_data = requests.get(url, timeout = SETTINGS['timeout']).text
				hash = md5.new(url).hexdigest()
				file_json_report = SETTINGS['output_path'] + f + '-report-' + hash + '.json.gz'
				with gzip.open(file_json_report, 'wb') as gzf:
					gzf.write(report_data.encode('ascii','ignore'))
			
			time.sleep(SETTINGS['benicewait'])
	
		except Exception, e:
			with open(SETTINGS['skip_file', 'a') as errorfile:
				errorfile.write('%s - Error scraping file: %s', (f, str(e)))
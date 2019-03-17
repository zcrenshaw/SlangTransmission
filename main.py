try:
    from urllib.request import urlopen
except ImportError:
	from urllib2 import urlopen

import csv

COUNT = 4;
SUBREDDIT = 5;

def getpage(url):
	page = urlopen(url)
	html = page.read()
	return html

def splitpage(page):
	return page.split("\n")

def getfield(lines, field):
	try:
		returnme = lines[field].split(":")[1].strip("\t ,[]")
	except IndexError:
		return ""
	return returnme.strip('"')

def main():
	terms = ['damn','shit']
	subs = ["hockey"]
	page_link = "http://api.pushshift.io/reddit/search/submission/?q=hello&aggs=subreddit&subreddit=askscience&after=5d"
	basic = ["http://api.pushshift.io/reddit/search/submission/?q=","&aggs=subreddit&subreddit=","&after=","d&before=","d"]
	fieldnames = ['Count','Subreddit']
	
	with open('output.csv', mode='w') as out:
		writer = csv.DictWriter(out, fieldnames=fieldnames)
		writer.writeheader()
		for i in range(10,100,10):
			query = basic[0]+terms[0]+basic[1]+subs[0]+basic[2]+str(i)+basic[3]+str(i-10)+basic[4]
			print(query)
			page = splitpage(getpage(query))
			count = getfield(page,COUNT)
			if count=="":
				count = 0;
			sub = subs[0]
			print(sub + '\t' + terms[0] + '\t' + str(count))	
			writer.writerow({fieldnames[0]: count, fieldnames[1]: sub})

main()

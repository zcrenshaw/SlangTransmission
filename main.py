#import requests
#from bs4 import BeautifulSoup
#import urllib2
try:
    from urllib.request import urlopen
except ImportError:
        from urllib2 import urlopen

COUNT = 4;
SUBREDDIT = 5;

def getpage(url):
	page = urlopen(url)
	html = page.read()
	return html

def splitpage(page):
	return page.split("\n")

def getfield(lines, field):
	returnme = lines[field].split(":")[1].strip("\t ,")
	return returnme.strip('"')

def main():
	page_link = "http://api.pushshift.io/reddit/search/submission/?q=hello&aggs=subreddit&subreddit=askscience&after=5d"
	page = splitpage(getpage(page_link))
	count = getfield(page,COUNT)
	sub = getfield(page,SUBREDDIT)
	print("Count: "+count+"\n"+"Subreddit: "+sub)

main()

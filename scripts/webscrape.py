# Zack Crenshaw
# Slang Transmission Project

# webscrape.py
# Scrape data from web based on given parameters

import sys
import requests
from time import sleep
import csv

# DATA COLLECTION ~~~~~~~~~~~~~~~~~~~~

def get_JSON(link,sleeptime):
    # scrapes the data given a link

    r = requests.get(link)

    if 'json' in r.headers.get('Content-Type'):
        try:
            data = r.json()
        except:
            print(link)
    else:
        if r.status_code == 429: # too many request error
            print("Too many requests: waiting " + str(sleeptime) + " seconds.")
            sleep(sleeptime)
            data = get_JSON(link)
        else:
            print("Error: Not in JSON format")
            data = None

    return data

def gen_link(args):
    # TODO: simplify link construction
    # constructs a link for search
    link = ['','']
    if args[1][0:5] == 'https': #if the input is a link
        link[0] = args[1]
        split = args[1].split('&')
        link[1] = split[0].split('/')[-2]
        for i in range(2,len(split)):
            link[1] += '_' + split[i]
    else: #if the inputs are parameters
        link[0] = 'https://api.pushshift.io/reddit/search/'
        link[0] += args[1] + '/?'
        link[1] += args[1]
        for i in range(2, len(args)):
            link[0] += '&' + args[i]
            if len(args[i]) == 0:
                # skip over error in text file commands
                continue
            if args[i] == '':
                link[1] += "_"
            if args[i][0:4] == "aggs":
                link[1] += '_' + args[i].split('=')[1]
            if "after" in args[i] or "before" in args[i] or 'subreddit' in args[i]:
                link[1] += '_' + args[i].split('=')[1]
            try:
                link[1] += str(int(args[i]))
            except:
                continue

    return link

def scrape(args, sleeptime):
    # given parameters, run a scrape

    # parameters (convention): sub/comment  (query)   subreddit   aggs    metadata   after   before

    link = gen_link(args) #produce link / filename

    data = get_JSON(link[0],sleeptime) #obtain data based on the link

    # ensures reading correct field even in case of no query
    if args[4].split('=')[0] == 'aggs':
        agg_type = args[4].split('=')[1]
    else:
        # no query is used for total
        agg_type = args[3].split('=')[1]

    if  agg_type == 'author':
        # return number of users using this term
        try:
            return [len(data['aggs']['author']),link[0]]
        except:
            return [0,link[0]]
    elif agg_type == 'subreddit':
        # return number of posts/comments with this term
        try:
            return [data['aggs']['subreddit'][0]['doc_count'],link[0]]
        except:
            return [0,link[0]]

    else:
        return "Not yet supported"



def batch(args):
    # given parameters, run a series of scrapes
    # will break the time interval into even buckets of a given size
    # parameters: sub/comment    query   subreddit   aggs   metadata    after   before  bucket_size

    SLEEPTIME = 1 # time for sleeping to account for 429 errors

    timescale = args[-3][-1]

    after = int(args[-3][6:-1])
    before = int(args[-2][7:-1])
    bucket_size = int(args[-1])

    header = ['count','average','after','before','link']
    name = create_filename(args)

    # sanity check on time scale
    if timescale != args[-2][-1]:
        print("Error: time not on same scale")
        exit(1)

    # ensures buckets are even size
    if (after - before) % bucket_size != 0:
        print("Error: buckets uneven")
        exit(1)

    a = after
    b = after - bucket_size

    search = args[:-3]
    search.extend(['',''])

    data = [] # to hold data

    # TODO: edit this link
    output = "../comment-data/" + name + ".csv"

    # collect data from the scrapes
    while (b >= before):
        search[-2] = 'after=' + str(a) + timescale
        search[-1] = 'before=' + str(b) + timescale
        point = scrape(search, SLEEPTIME)
        data.append([point[0],'NA',a,b,point[1]])
       # sleep(SLEEPTIME / 10)  # too prevent 429 (too many request) errors
        a = b
        b = a - bucket_size

    # write to csv file
    with open(output, 'w') as file:
        filewriter = csv.writer(file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(header)
        for i in range(len(data)):
            filewriter.writerow(data[i])




# CSV GENERATION ~~~~~~~~~~


def create_filename(args):
    name = ""

    name += args[1]
    for i in range(2, len(args) - 1):
        if args[i] == '':
            name += "_"
        if args[i][0:4] == "aggs":
            name += "_" + args[i].split('=')[1]
        if args[i][0:2] == 'q=':
            name += "_" + args[i].split('=')[1]
        if "after" in args[i] or "before" in args[i] or 'subreddit' in args[i]:
            name += "_" + args[i].split('=')[1]
        try:
            name += "_" + str(int(args[i]))
        except:
            continue

    name += "_" + str(args[-1])

    return name


# merges two csv files
def merge(infile, outfile):
    needHeader = False # to check if file needs a header (is empty or non-existent)
    with open(infile,'r') as input:
        try: #check for file exist
            with open(outfile, 'r') as check:
                #check for file empty
                needHeader = len(check.readline().strip('\n').split(',')) < 2
        except:
            #if does not exist, mark as needing header
            needHeader = True

        with open(outfile,'a+') as output:
            filewriter = csv.writer(output, delimiter=',',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)

            inline = input.readline().strip('\n').split(',')

            # if new or empty document create and/or add header
            if needHeader:
                filewriter.writerow(inline)

            inline = input.readline().strip('\n').split(',')

            # iterate through input, append to output
            while (len(inline) > 1): #check for EOF
                filewriter.writerow(inline)
                inline = input.readline().strip('\n').split(',')


# RUN MULTIPLE SEARCHES FROM TEXT FILE
def scrape_from_txt(file):
    with open(file, 'r') as textfile:
        command = textfile.readline().strip().split(' ')
        while len(command) > 1: # check for EOF
            batch(command)
            print(command[0] + " is done")
            command = textfile.readline().strip().split(' ')

# ~~~~~~~~~~~~~

if __name__ == '__main__':
        # run scrapes from text file if that is the input
        if sys.argv[1][-4:-1] == ".tx":
            print("Running batch of scrapes from text file")
            scrape_from_txt(sys.argv[1])
            print("All done!")

        # if need to merge files
        elif sys.argv[1] == 'merge':
            merge(sys.argv[2],sys.argv[3])

        # else just run the scrape with the given parameters
        else:
            print("Running scrape from given parameters")
            batch(sys.argv)
            print("Done")


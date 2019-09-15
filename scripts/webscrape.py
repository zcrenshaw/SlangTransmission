# Zack Crenshaw
# Slang Transmission Project

# webscrape.py
# Scrape data from web based on given parameters

import sys
import requests
from time import sleep
import csv
import traceback
import pandas as pd
import numpy as np
from math import ceil

check_folder = True
nodes = 0

# DATA COLLECTION ~~~~~~~~~~~~~~~~~~~~

def get_JSON(link,sleeptime,attempts):
    # scrapes the data given a link

    if attempts == 0:
        print("Error in accessing data – no attempts left, aborting")
        exit(1)

    try:
        r = requests.get(link)
    except:
        try:
            print(r)
        except:
            print("Issue with link: attempting again")
            sleep(sleeptime)
            return get_JSON(link,sleeptime,attempts-1)


    if 'json' in r.headers.get('Content-Type'):
        try:
            data = r.json()
        except:
            print("Error: Issue with JSON formatting")
    else:
        if r.status_code == 429: # too many request error
            print("Too many requests: waiting " + str(sleeptime) + " seconds.")
            sleep(sleeptime)
            data = get_JSON(link,sleeptime,attempts)
        else:
            print("Error: Not in JSON format")
            print(r.headers.get('Content-Type'))
            print(r.text)
            data = None

    return data

def gen_link(args):
    # constructs a link for search
    link = ''
    if args[1][0:5] == 'https': # if the input is a link
        link = args[1]
    else: # if the inputs are parameters
        link = 'https://api.pushshift.io/reddit/search/'
        link += args[1] + '/?'
        for i in range(2, len(args)):
            if len(args[i]) == 0:
                # skip over error in text file commands
                continue
            else:
                link += '&' + args[i]

    return link

def convert_to_secs(x,scale):
    if scale == 'd':
        return x * 24 * 60 * 60
    elif scale == 'h':
        return x * 60 * 60
    elif scale == 'm':
        return x * 60
    elif scale == 's':
        return x
    else:
        print("Error: timescale not valid")
        exit(0)


def create_subs(after,before,sections):
    a = int(after.split('=')[1][:-1])
    b = int(before.split('=')[1][:-1])
    timescale = after[-1]
    a = convert_to_secs(a,timescale)
    b = convert_to_secs(b,timescale)
    return np.linspace(a,b,num=sections,dtype=int)

def create_labels(a,b):
    after = 'after=' + str(a) + 's'
    before = 'before=' + str(b) + 's'
    return [after,before]



def scrape(args,sleeptime,attempts,goal,current):
    # given parameters, run a scrape

    # parameters (convention): sub/comment  (query)   subreddit   aggs    metadata .... score   after   before

    link = gen_link(args)  # produce link / filename

    data = get_JSON(link, sleeptime,attempts)  # obtain data based on the link

    return_data = [None] * 3

    arg_copy = args

    df = None

    max = 1000

    global nodes

    # ensures reading correct field even in case of no query
    if args[4].split('=')[0] == 'aggs':
        agg_type = args[4].split('=')[1]
    else:
        # no query is used for total
        agg_type = args[3].split('=')[1]

    # INFLUENCER FINDER
    if agg_type == 'none':
        nodes+=1
        total = data['metadata']['total_results']
        # create list of text
        if goal < 0:
            #first access for this query
            goal = total/100
            current = 0
        page = data['data']
        text = []
        lpage = len(page)
        maxscore = sys.maxsize
        foundend = False

        if lpage == 0: # no hits
            text.append('')
            foundend = True

        if args[1] == 'submission':
            # if post
            field = 'title'
            nextfield = 'selftext'
        else:
            # if comment
            field = 'body'
            nextfield = None

        for i in range(lpage):
            current += 1
            if current > goal:
                foundend = True
                break
            addtext = page[i][field]
            if nextfield is not None:
                try:
                    addtext += '\n\n\n' + page[i][nextfield]
                except:
                    pass

            text.append(addtext)
            maxscore = page[i]['score']

        df = pd.DataFrame(pd.Series(data=text,name='text'))

        if not foundend:
            args[-3] = 'score=<{}'.format(maxscore)
            lower = scrape(args,sleeptime,attempts,goal,current)
            df = df.append(lower[2])

        return_data[0] = -1

    # USERS FINDER

    elif agg_type == 'author':
        # return number of users using this term
        try:
            total = data['metadata']['total_results']
            df = pd.DataFrame(columns=['author', 'count'])
            if total > max: # true max is 1000, but can leave wiggle room
                sections = ceil(total/(max*.8))
                dates = create_subs(args[-2],args[-1],sections+1)
                for i in range(sections):
                    zone = create_labels(dates[i],dates[i+1])
                    arg_copy[-2] = zone[0]
                    arg_copy[-1] = zone[1]
                    section_data = scrape(arg_copy,sleeptime,attempts,0,0)[2]
                    df = df.merge(section_data,on='author',suffixes=['_l','_r'],how='outer')
                    df = df.replace(to_replace=np.nan,value=0)
                    df['count'] = df['count_l'] + df['count_r']
                    df = df.drop(['count_l','count_r'],axis=1)

                df = df.sort_values(by='count',ascending=False)
                return_data[0] = len(df.index)

            else:
                authors = data['aggs']['author']
                users = []
                counts = []
                for a in authors:
                    users.append(a['key'])
                    counts.append(a['doc_count'])

                if len(users) == 0:
                    users.append('')
                    counts.append(0)

                frame = {'author': pd.Series(users), 'count': pd.Series(counts)}
                df = pd.DataFrame(frame)
                return_data[0] = len(df.index)

            nodes += 1
            if nodes % 100 == 0:
                print(str(nodes) + " accesses complete")
            if nodes%2000 == 0:
                print(df)

        except Exception as e:
            print('FAIL')
            #print("Error: ", sys.exc_info()[0], "occurred.")
            #print(e)
            traceback.print_exc()
            exit(0)
            return_data[0] = 0

    # TOTAL CONTENT FINDER
    elif agg_type == 'subreddit':
        # return number of posts/comments with this term
        try:
            return_data[0] = data['aggs']['subreddit'][0]['doc_count']
        except:
            return_data[0] = 0

    else:
        return "Not yet supported"

    return_data[1] = link
    return_data[2] = df
    return return_data

def batch(args,out_folder):
    # given parameters, run a series of scrapes
    # will break the time interval into even buckets of a given size
    # parameters: sub/comment    query   subreddit   aggs  fields metadata    after   before  bucket_size

    SLEEPTIME = 3 # time for sleeping to account for 429 errors
    ATTEMPTS = 5
    global nodes

    timescale = args[-3][-1]

    after = int(args[-3][6:-1])
    before = int(args[-2][7:-1])
    bucket_size = int(args[-1])

    header = ['count','after','before','link']
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

    ''' # checking output folder
    global check_folder
    if (check_folder):
        response = input("Is " + out_folder + " the correct destination? Y/N \n")
        if response.lower() == 'y' or response.lower() == 'yes':
            check_folder = False
            print("Moving forward")
        else:
            print("Please input correct destination folder and run again.")
            exit(0)
    
    '''

    output = out_folder + name + ".csv"

    writeout = True

    # collect data from the scrapes
    while (b >= before):
        search[-2] = 'after=' + str(a) + timescale
        search[-1] = 'before=' + str(b) + timescale
        point = scrape(search, SLEEPTIME,ATTEMPTS,-1,0)
        if point[0] == -1:
            output = out_folder + create_filename(search + [bucket_size]) + ".csv"
            point[2].to_csv(output,index=False)
            writeout = False
        data.append([point[0],a,b,point[1]])
        sleep(SLEEPTIME / 50)  # to prevent 429 (too many request) errors
        a = b
        b = a - bucket_size


    # write to csv file
    if writeout:
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
        if "after" in args[i] or "before" in args[i] or 'subreddit=' in args[i]:
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
                # check for file empty
                needHeader = len(check.readline().strip('\n').split(',')) < 2
        except:
            # if does not exist, mark as needing header
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
def scrape_from_txt(file,out_folder):
    with open(file, 'r') as textfile:
        command = textfile.readline().strip().split(' ')
        while len(command) > 1: # check for EOF
            batch(command,out_folder)
            print(command[0] + " is done")
            global nodes
            nodes = 0
            command = textfile.readline().strip().split(' ')

# ~~~~~~~~~~~~~

if __name__ == '__main__':
        # run scrapes from text file if that is the input
        if sys.argv[1][-4:-1] == ".tx":
            print("Running batch of scrapes from text file")
            scrape_from_txt(sys.argv[1],sys.argv[2])
            print("All done!")

        # if need to merge files
        elif sys.argv[1] == 'merge':
            merge(sys.argv[2],sys.argv[3])

        # else just run the scrape with the given parameters
        else:
            print("Running scrape from given parameters")
            batch(sys.argv,"./")
            print("Done")


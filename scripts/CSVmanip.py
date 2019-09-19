# Zack Crenshaw
# Slang Transmission Project

# CSVmanip.py
# Manipulate CSVs

import sys
import os
import pandas as pd
import re


def prep_df(filename,droplink):
    # prepares a data frame for more processing
    # prunes some metadata away
    try:
        df = pd.read_csv(filename)
        if droplink:
            try:
                df = df.drop("link",axis=1)
            except:
                print("No column: 'link' ")
        mid = list()
        for i in range(len(df['after'])):
            mid.append(df[['after','before']].iloc[i].mean())
        df['mid'] = mid
        df.set_index(['mid'],drop=True,inplace=True)
        return df
    except:
        print("Error in data frame production: ", filename)



def rolling_average(df,smoothing):
    # computes a rolling average over a data set
    df['avg'] = pd.Series([])
    df['avg'] = df['count'].rolling(window=smoothing, center=True).mean()
    return df

def append_data(df1,name1,df2,name2):
    # appends data together
    # df1 input
    # df2 output
    df2[name2] = df1[name1]
    return df2

def del_col(df,col):
    # deletes a column
    try:
        df = df.drop(col,axis=1)
    except:
        print("Column not found")
    return df


def count_influence(q,src,dst):
    # counts the number of influencers that contain term q
    after = []
    before = []
    sums = []

    for file in os.listdir(src):
        if file[-3:] == 'csv':
            df = pd.read_csv(src+file)
            file_split = file.split('_')
            a = file_split[-3][:-1]
            b = file_split[-2][:-1]
            df['count'] = df["text"].str.count(q, re.I)
            count = df['count'].sum()
            after.append(a)
            before.append(b)
            sums.append(count)

    frame = {'after': after,'before': before, 'count': sums}

    df = pd.DataFrame(frame)

    df.to_csv(dst,index=False)

def arith(src,cols,val,dst):
    # computes arithmetic on columns of given file
    df = pd.read_csv(src)
    for c in cols:
        try:
            df[c] += val
        except:
            print("Column not present, but resaving file in new destination")
            break
    df.to_csv(dst)

def make_adjustment(src,cols,mod):
    # rounds timescale to number divisble by mod
    # rounds down
    # does adjustment on one file
    val = int(src.split('_')[-2][:-1])
    delta = val % mod
    if delta != 0:
        # if change is needed
        delta *= -1
        dst = adjust_dst(src,delta)
        arith(src, cols,delta,dst)

def adjust_dst(src,delta):
    # creates new path based on the delta
    editpath = src.split('_') # isolate parameters
    # adjust numbers
    editpath[-2] = str(int(editpath[-2][:-1]) + delta) + editpath[-2][-1]
    editpath[-3] = str(int(editpath[-3][:-1]) + delta) + editpath[-3][-1]
    # rejoin string name
    editpath[-2] = ''.join(editpath[-2])
    editpath[-3] = ''.join(editpath[-3])
    dst = '_'.join(editpath)
    return dst


def adjust_timescale(src,cols,mod):
    # rounds timescale to number divisble by mod
    # does adjustment to entire folder
    if src[-3:] == 'csv':
        make_adjustment(src,cols,mod)
    elif os.path.isdir(src):
        for file in os.listdir(src):
            adjust_timescale(src+file,cols,mod)
    else:
        print(src[-3:])
        print("Error: file must be .csv format – did not execute")


def manip():
    # main function, executes the desired manipulations
    func = sys.argv[1]

    if func == 'roll-avg':
        # calculate rolling average of a file
        file = sys.argv[2]
        smoothing = int(sys.argv[3])
        rolling_average(prep_df(file,False),smoothing).to_csv(file)
    elif func == 'roll-avg-dir':
        # calculate rolling averages of a whole directory
        direc = sys.argv[2]
        smoothing = int(sys.argv[3])
        for filename in os.listdir(direc):
            if not filename.endswith('.csv'):
                continue
            filepath = direc + filename
            rolling_average(prep_df(filepath,False),smoothing).to_csv(filepath)
    elif func == 'append-data':
        # append a column to another data set
        infile = sys.argv[2]
        incol = sys.argv[3]
        outfile = sys.argv[4]
        outcol = sys.argv[5]
        append_data(prep_df(infile,False),incol,prep_df(outfile,False),outcol).to_csv(outfile)
    elif func == 'del-col':
        # delete a column
        file = sys.argv[2]
        col = sys.argv[3]
        del_col(prep_df(file,False),col).to_csv(file)
    elif func == 'del-col-dir':
        # delete a column of a whole directory
        direc = sys.argv[2]
        col = sys.argv[3]
        for filename in os.listdir(direc):
            filepath = direc + filename
            if not filename.endswith('.csv'):
                continue
            del_col(prep_df(filepath,False),col).to_csv(filepath)
    elif func == 'concat':
        # concatenate column from all other files in folder onto one file
        direc = sys.argv[2]
        incol = sys.argv[3]
        outfile = sys.argv[4]
        outpath = direc + outfile
        outdf = prep_df(outpath,True)
        for filename in os.listdir(direc):
            filepath = direc + filename
            if not filename.endswith('.csv') or filepath == outpath:
                continue
            # get term
            outcol = filename.split('_')[1]
            # append data
            outdf = append_data(prep_df(filepath,True),incol,outdf,outcol)
            outdf.to_csv(outpath[:-4]+"_ALL.csv")
    elif func == "append":
        # append files together, output to oufile
        file1 = pd.read_csv(sys.argv[2])
        file2 = pd.read_csv(sys.argv[3])
        outfile = sys.argv[4]

        output = file1.append(file2)
        output.to_csv(outfile)

    elif func == "count-influence":
        # count the occurence of a term in an influencer file
        q = sys.argv[2]
        folder = sys.argv[3]
        output = sys.argv[4]
        count_influence(q,folder,output)

    elif func == "arith":
        # adds val to a column
        src = sys.argv[2]
        cols = sys.argv[3]
        val = sys.argv[4]
        arith(src,cols,val,src)
    elif func == "timescale":
        # adjusts timescale on a file or folder
        # cols should be "after" and "before" for timescale adjustment
        # file path name only works for timescale adjustment
        src = sys.argv[2]
        cols = sys.argv[3].split(',')
        mod = int(sys.argv[4])
        adjust_timescale(src,cols,mod)

    else:
        print("Not yet supported")




if __name__ == '__main__':
    manip()
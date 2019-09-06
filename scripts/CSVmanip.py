# Zack Crenshaw
# Slang Transmission Project

# CSVmanip.py
# Manipulate CSVs

import sys
import os
import pandas as pd


def prep_df(filename,droplink):
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
    df['avg'] = pd.Series([])
    df['avg'] = df['count'].rolling(window=smoothing, center=True).mean()
    return df

def append_data(df1,name1,df2,name2):
    # df1 input
    # df2 output
    df2[name2] = df1[name1]
    return df2

def del_col(df,col):
    try:
        df = df.drop(col,axis=1)
    except:
        print("Column not found")
    return df


def manip():
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
        file1 = pd.read_csv(sys.argv[2])
        file2 = pd.read_csv(sys.argv[3])
        outfile = sys.argv[4]

        output = file1.append(file2)
        output.to_csv(outfile)



    else:
        print("Not yet supported")




if __name__ == '__main__':
    manip()
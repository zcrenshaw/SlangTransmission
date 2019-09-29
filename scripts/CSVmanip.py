# Zack Crenshaw
# Slang Transmission Project

# CSVmanip.py
# Manipulate CSVs

import sys
import os
import pandas as pd
import glob
import traceback
from subprocess import call


def prep_df(filename, droplink):
    # prepares a data frame for more processing
    # prunes some metadata away
    try:
        df = pd.read_csv(filename)
        if droplink:
            try:
                df = df.drop("link", axis=1)
            except:
                print("No column: 'link' ")
        mid = []
        for i in range(len(df['after'])):
            mid.append(df[['after', 'before']].iloc[i].mean())
        # deletes rogue index column
        for c in df.columns:
            if 'Unnamed' in c:
                df = df.drop(c, axis=1)
        df['mid'] = mid
        df.set_index(['mid'], drop=True, inplace=True)
        return df
    except:
        print("Error in data frame production: ", filename)


def rolling_average(df, smoothing, cols):
    # computes a rolling average over a data set
    for c in cols:
        df[c + '-avg'] = pd.Series([])
        df[c + '-avg'] = df[c].rolling(window=smoothing, center=True).mean()
    return df


def concat_batcher(direc, incol):
    # concats a file together

    # finds main file
    for filename in os.listdir(direc):
        if len(filename.split('_')) == 6:
            outpath = direc + filename

    outdf = prep_df(outpath, True)
    outdf['total'] = outdf['count']
    outdf = del_col(outdf, 'count')

    for filename in os.listdir(direc):
        filepath = direc + filename
        if not filename.endswith('.csv') or filepath == outpath:
            continue
        # get term
        outcol = filename.split('_')[1]
        # append data
        outdf = concat(prep_df(filepath, False), incol, outdf, outcol)
        # reorder
    cols = outdf.columns.tolist()
    cols.insert(0, cols.pop(cols.index('after')))
    cols.insert(1, cols.pop(cols.index('before')))
    outdf = outdf[cols]
    outdf.to_csv(outpath[:-4] + "_ALL.csv", index=False)


def concat(df1, name1, df2, name2):
    # appends columns together
    # df1 input
    # df2 output
    if name1 == name2:
        name2 += "_r"
    df2[name2] = df1[name1]
    return df2


def append_data(df1, df2):
    # appends data together (row-wise)
    # df1 input
    # df2 output
    df1 = df1.append(df2)
    return df1


def del_col(df, col):
    # deletes a column
    try:
        df = df.drop(col, axis=1)
    except:
        print("Column not found")
    return df


def count_influence(q, src, dst):
    # counts the number of influencers that contain terms q
    cols = ['after', 'before']
    cols.extend(q)
    outdf = pd.DataFrame(columns=cols)

    for file in os.listdir(src):
        if file.endswith('.csv'):
            file_split = file.split('_')
            a = file_split[-3][:-1]
            b = file_split[-2][:-1]
            indf = pd.read_csv(src + file)
            data = dict.fromkeys(cols, 0)
            for index, row in indf.iterrows():
                for term in q:
                    if term == 'total':
                        data['total'] += 1
                    elif term in row['text']:
                        data[term] += 1
            data['after'] = a
            data['before'] = b
            outdf = outdf.append(data, ignore_index=True)

    outdf.to_csv(dst, index=False)


def arith(src, cols, val, dst):
    # computes arithmetic on columns of given file
    df = pd.read_csv(src)
    for c in cols:
        try:
            df[c] += val
        except:
            print("Column not present, but resaving file in new destination")
            break
    df.to_csv(dst, index=False)


def make_adjustment(src, cols, mod):
    # rounds timescale to number divisble by mod
    # rounds down
    # does adjustment on one file
    val = int(src.split('_')[-2][:-1])
    delta = val % mod
    if delta != 0:
        # if change is needed
        delta *= -1
        dst = adjust_dst(src, delta)
        arith(src, cols, delta, dst)
        os.remove(src)


def adjust_dst(src, delta):
    # creates new path based on the delta
    editpath = src.split('_')  # isolate parameters
    # adjust numbers
    editpath[-2] = str(int(editpath[-2][:-1]) + delta) + editpath[-2][-1]
    editpath[-3] = str(int(editpath[-3][:-1]) + delta) + editpath[-3][-1]
    # rejoin string name
    editpath[-2] = ''.join(editpath[-2])
    editpath[-3] = ''.join(editpath[-3])
    dst = '_'.join(editpath)
    return dst


def adjust_timescale(src, cols, mod):
    # rounds timescale to number divisble by mod
    # does adjustment to entire folder
    try:
        if src[-3:] == 'csv':
            make_adjustment(src, cols, mod)
        elif os.path.isdir(src):
            for file in os.listdir(src):
                newpath = src + file
                if os.path.isdir(newpath):
                    newpath += '/'
                adjust_timescale(newpath, cols, mod)
        else:
            print(src)
            print("Error: file must be .csv format – did not execute")
    except:
        print(src)
        traceback.print_exc()


def roll_avg_dir(direc, smoothing, cols, target):
    for filename in os.listdir(direc):
        filepath = direc + filename
        if target is not None:
            if target in filename:
                if filename.endswith('.csv'):
                    rolling_average(prep_df(filepath, False), smoothing, cols).to_csv(filepath, index=True)


def folders_in(path_to_parent):
    for fname in os.listdir(path_to_parent):
        if os.path.isdir(os.path.join(path_to_parent,fname)):
            yield os.path.join(path_to_parent,fname)

def build_tree(folder,ignore):
    direcs = list(folders_in(folder))
    temp = []
    for d in direcs:
        if ignore not in d:
            sub = list(folders_in(d))
            if len(sub) == 0:
                temp.append(d)
            else:
                temp.extend(build_tree(d,ignore))
    return temp


def manip():
    # main function, executes the desired manipulations
    func = sys.argv[1]

    if func == 'roll-avg':
        # calculate rolling average of a file
        file = sys.argv[2]
        smoothing = int(sys.argv[3])
        cols = sys.argv[4].split(',')
        rolling_average(prep_df(file, False), smoothing, cols).to_csv(file, index=True)

    elif func == 'roll-avg-dir':
        direc = sys.argv[2]
        smoothing = int(sys.argv[3])
        cols = sys.argv[4].split(',')
        target = sys.argv[5]
        roll_avg_dir(direc,smoothing,cols,target)

    elif func == 'roll-avg-r':
        # calculate rolling averages of a whole directory and subdirectories
        # ignore all directories and files with avoid in the name
        root = sys.argv[2]
        ignore = sys.argv[3]
        smoothing = int(sys.argv[4])
        cols = sys.argv[5].split(',')
        if len(sys.argv) == 7:
            target = sys.argv[6]
        else:
            target = None
        # find subfolders
        tree = build_tree(root, ignore)
        # leaf folder: folder with no folders inside
        # if the folder is a leaf folder, make it the tree
        if len(tree) == 0:
            tree = [root]
        for leaf in tree:
            if leaf[-1] != '/':
                leaf += '/'
            try:
                roll_avg_dir(leaf, smoothing, cols, target)
            except:
                print(leaf)
                traceback.print_exc()

    elif func == 'append-data':
        # append a column to another data set
        infile = sys.argv[2]
        incol = sys.argv[3]
        outfile = sys.argv[4]
        outcol = sys.argv[5]
        concat(prep_df(infile, False), incol, prep_df(outfile, False), outcol).to_csv(outfile, index=False)
    elif func == 'append-file':
        # append file to end of other
        infile = sys.argv[2]
        outfile = sys.argv[3]
        append_data(prep_df(infile, False), prep_df(outfile, False)).to_csv(outfile, index=False)
    elif func == 'append-dir':
        folder = sys.argv[2]
        outfile = sys.argv[3]
        filelist = sorted(glob.glob(os.path.join(folder, '*.csv')))
        df = prep_df(filelist[0], False)
        for i in range(1, len(filelist)):
            df = append_data(df, prep_df(filelist[i], False))

        df.to_csv(outfile, index=False)

    elif func == 'del-col':
        # delete a column
        file = sys.argv[2]
        col = sys.argv[3]
        del_col(prep_df(file, False), col).to_csv(file, index=False)
    elif func == 'del-col-dir':
        # delete a column of a whole directory
        direc = sys.argv[2]
        col = sys.argv[3]
        for filename in os.listdir(direc):
            filepath = direc + filename
            if not filename.endswith('.csv'):
                continue
            del_col(prep_df(filepath, False), col).to_csv(filepath, index=False)
    elif func == 'concat':
        # concatenate column from all other files in folder onto one file
        direc = sys.argv[2]
        if direc[-1] != '/':
            direc += '/'
        incol = sys.argv[3]
        concat_batcher(direc, incol)

    elif func == 'concat-r':
        # calls concat on all subfolders
        root = sys.argv[2]
        ignore = sys.argv[3]
        incol = sys.argv[4]

        # find subfolders
        tree = build_tree(root, ignore)
        # leaf folder: folder with no folders inside
        # if the folder is a leaf folder, make it the tree
        if len(tree) == 0:
            tree = [root]
        for leaf in tree:
            if leaf[-1] != '/':
                leaf += '/'
            try:
                concat_batcher(leaf, incol)
            except:
                print(leaf)
                traceback.print_exc()


    elif func == "append":
        # append files together, output to oufile
        file1 = pd.read_csv(sys.argv[2])
        file2 = pd.read_csv(sys.argv[3])
        outfile = sys.argv[4]

        output = file1.append(file2)
        output.to_csv(outfile, index=False)

    elif func == "count-influence":
        # count the occurence of a term in an influencer file
        q = sys.argv[2].split(',')
        folder = sys.argv[3]
        output = sys.argv[4]
        try:
            count_influence(q, folder, output)
        except:
            print(sys.argv)
            traceback.print_exc()

    elif func == "arith":
        # adds val to a column
        src = sys.argv[2]
        cols = sys.argv[3].split(',')
        val = int(sys.argv[4])
        arith(src, cols, val, src)

    elif func == 'arith-dir':
        # adds val to column in entire folder
        # used to adjust times (adjust_dst)
        src = sys.argv[2]
        cols = sys.argv[3].split(',')
        val = int(sys.argv[4])
        for file in os.listdir(src):
            if file.endswith('.csv'):
                dst = adjust_dst(src + file, val)
                arith(src + file, cols, val, dst)
                os.remove(src + file)

    elif func == "timescale":
        # adjusts timescale on a file or folder
        # cols should be "after" and "before" for timescale adjustment
        # file path name only works for timescale adjustment
        src = sys.argv[2]
        cols = sys.argv[3].split(',')
        mod = int(sys.argv[4])
        adjust_timescale(src, cols, mod)

    elif func == "batch":
        file = sys.argv[2]
        print("Running batch of manipulations")
        with open(file, 'r') as textfile:
            command = textfile.readline().strip().split(' ')
            while len(command) > 1:  # check for EOF
                call(command)
                command = textfile.readline().strip().split(' ')

    else:
        print("Not yet supported")


if __name__ == '__main__':
    manip()
    print("Completed")

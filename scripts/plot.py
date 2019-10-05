# Zack Crenshaw
# Slang Transmission Project

# plot.py
# plot from CSV files


import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
from CSVmanip import build_tree, folders_in
import traceback


def make_title(path):
    # create title for plot
    filename = path.split('/')[-1]
    filename = filename.split('_')
    # type of content
    if filename[0] == 'comment':
        content = 'Comments'
    else:
        content = 'Posts'
    sub = filename[1]
    # type of aggregation
    if filename[2] == 'author':
        agg = 'User'
        label = 'Users'
    elif filename[2] == 'influence':
        agg = 'Influencer'
        label = 'Influencers'
    elif filename[2] == 'subreddit':
        agg = 'Total ' + content
        label = agg
    else:
        type = 'Term: ' + filename[2]

    title = "{}: {} By {}".format(sub,content,agg)
    name = [sub,content,agg]

    return title,name,"Percent of {}".format(label)


def plot(file,direc):
    # plot a file
    if direc[-1] != '/':
        direc += '/'

    df = pd.read_csv(direc + file)

    cols = list(df.columns.values)
    lencols = len(cols)

    # calculate percentages of total
    for i in range(12, lencols):
        df[cols[i] + "%"] = (df[cols[i]] / df['total-avg']) * 100

    # create subset of data for plotting
    sub = df[['mid', 'deadass-avg%', 'def-avg%', 'defo-avg%', 'periodt-avg%', 'periot-avg%', 'stan-avg%', 'thot-avg%']]
    mylabels = ['deadass', 'def', 'defo', 'periodt', 'periot', 'stan', 'thot']

    title,name,y_label = make_title(file)

    sub.plot(x='mid', title=title).invert_xaxis()
    plt.suptitle = title
    plt.xlabel("Days ago")
    plt.ylabel(y_label)
    plt.legend(labels=mylabels,loc='upper left')
    savefile = direc + '_'.join(name)
    plt.savefig(savefile)
    plt.close()

def plot_by_hand(file, output, title, xlabel, y1label, y2label, term):
    # plot a file with hand selected inputs

    df = pd.read_csv(file)
    cols = list(df.columns.values)

    myLabels = []
    sub_cols = ['mid']

    for i in range(len(cols)):
        if term in cols[i]:
            # repetitive code – should be reduced
            if 'comment' in cols[i]:
                df[cols[i] + "%"] = (df[cols[i]] / df['total-comment-avg']) * 100
                sub_cols.append(cols[i] + "%")
                if cols[i].endswith('-avg'):
                    myLabels.append(cols[i][:-4])
                else:
                    myLabels.append(cols[i])
            elif 'post' in cols[i]:
                df[cols[i] + "%"] = (df[cols[i]] / df['total-post-avg']) * 100
                sub_cols.append(cols[i] + "%")
                if cols[i].endswith('-avg'):
                    myLabels.append(cols[i][:-4])
                else:
                    myLabels.append(cols[i])
            elif 'influencer' in cols[i]:
                df[cols[i] + "%"] = (df[cols[i]] / df['total-influencer-avg']) * 100
                sub_cols.append(cols[i] + "%")
                if cols[i].endswith('-avg'):
                    myLabels.append(cols[i][:-4])
                else:
                    myLabels.append(cols[i])
            elif 'user' in cols[i]:
                df[cols[i] + "%"] = (df[cols[i]] / df['total-user-avg']) * 100
                sub_cols.append(cols[i] + "%")
                if cols[i].endswith('-avg'):
                    myLabels.append(cols[i][:-4])
                else:
                    myLabels.append(cols[i])

    sub = df[sub_cols]

    sub.plot(x='mid', y=sub_cols[1:], title=title).invert_xaxis()
    plt.suptitle = title
    plt.xlabel(xlabel)
    plt.ylabel(y1label)
    plt.legend(labels=myLabels, loc='upper right')
    # there is a bug here that creates an extra line
    ax = df[sub_cols[-1]].plot(secondary_y=True)
    ax.set_ylabel(y2label)

    plt.show()
    '''
    plt.savefig(output)
    plt.close()
    '''

def find_file(direc,target):
    # find file and plot it
    if os.path.isdir(direc):
        for file in os.listdir(direc):
            if target in file:
                print('Plotting: ',file, direc)
                plot(file, direc)


def recursive(root,ignore,target):
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
            find_file(leaf, target)
        except:
            print(leaf)
            traceback.print_exc()


def main():
    func = sys.argv[1]

    if func == '-r':
        # recursive plot
        root = sys.argv[2]
        ignore = sys.argv[3]
        if len(sys.argv) == 5:
            target = sys.argv[4]
        else:
            target = None
        recursive(root,ignore,target)
    elif func == '-s':
        # single plot
        file = sys.argv[2]
        direc = sys.argv[3]
        plot(file,direc)

    elif func == 'by-hand':
        # plots content and user vs influencer (by hand using custom csv)
        file = sys.argv[2]
        output = file.split('.')[0]
        title = input('Title:\n')
        xlabel = input('X Label:\n')
        y1label = input('Y1 Label:\n')
        y2label = input('Y2 Label:\n')

        '''
        title = sys.argv[3]
        xlabel = sys.argv[4]
        y1label = sys.argv[5]
        y2label = sys.argv[6]
        '''
        term = sys.argv[3]
        plot_by_hand(file, output, title, xlabel, y1label, y2label, term)







if __name__ == '__main__':
    main()
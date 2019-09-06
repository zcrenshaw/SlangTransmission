# Zack Crenshaw
# Slang Transmission Project

# plot.py
# plot from CSV files


import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def make_title(path):
    filename = path.split('/')[-1]
    filename = filename.split('_')
    return [filename[0],filename[1]]

def main():
    file = sys.argv[1]
    df = pd.read_csv(file)

    cols = list(df.columns.values)
    lencols = len(cols)
    max = 0

    # calculate percentages of total
    for i in range(5,lencols):
        df[cols[i]+"%"] = (df[cols[i]]/df['avg']) * 100
        max = df[cols[i]+"%"].max()

    # create subset of data for plotting
    sub = df[['mid','deadass%','def%','defo%','periodt%','periot%','stan%','thot%']]

    proto_title = make_title(file)
    title = proto_title[0] + ": " + proto_title[1]
    sub.plot(x='mid',title=title).invert_xaxis()
    plt.suptitle = title
    plt.xlabel("Days ago")
    plt.ylabel("Percent of total posts")
    savefile = '/'.join(file.split('/')[:-1]) + '/' + '_'.join(proto_title)
    plt.savefig(savefile)

if __name__ == '__main__':
    main()
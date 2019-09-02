# Zack Crenshaw
# Slang Transmission Project

# CSVmanip.py
# Manipulate CSVs

import sys
import os
import csv

def compute_rolling_average(data,smoothing):
    # construct rolling average
    # currently algorithmically slow TODO: improve time complexity
    for i in range(smoothing//2,len(data)-smoothing//2):
        local_total = 0
        for j in range(-(smoothing//2),smoothing//2+1):
            local_total += int(data[i+j][0])
        data[i][1] = local_total / smoothing

    return data

def rolling_average(filename,smoothing):
    data = []
    with open(filename,'r') as readfile:
        header = readfile.readline().strip('\n').split(',')
        line = readfile.readline().strip('\n').split(',')
        while (len(line) > 1): # check for EOF
            data.append(line)
            line = readfile.readline().strip('\n').split(',')
    data = compute_rolling_average(data,smoothing)
    with open(filename,'w+') as file:
        filewriter = csv.writer(file, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        filewriter.writerow(header)
        for i in range(len(data)):
            filewriter.writerow(data[i])

def manip():
    func = sys.argv[1]

    if func == 'roll-avg':
        rolling_average(sys.argv[2],int(sys.argv[3]))
    if func == 'roll-avg-dir':
        for filename in os.listdir(sys.argv[2]):
            rolling_average(sys.argv[2]+ filename,int(sys.argv[3]))



if __name__ == '__main__':
    manip()
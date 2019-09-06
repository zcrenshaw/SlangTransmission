import sys
from subprocess import call

def main():
    subs = open(sys.argv[1]).readlines()
    folder = sys.argv[2]
    for s in subs:
        s = s.strip('\n')
        call(["cd",folder])
        #print(["mv","-v",folder+ '*'+s+"*.csv",folder+s+'/'])
        call(["mv","./*"+s+"*.csv",s+"/"])

main()
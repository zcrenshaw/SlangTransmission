# Zack Crenshaw
# Slang Transmission

#commandline.py
#	designed to take in multiple commands to run


import sys
from subprocess import call

def main():
	file = sys.argv[1]
	cmds = []
	with open(file,'r') as src:
		c = src.readline().strip().split(' ')
		while len(c) > 1:
			cmds.append(c)
			c = src.readline().strip().split(' ')
		
	for c in cmds:
		call(c)

main()



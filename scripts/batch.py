# Zack Crenshaw

# Will run a batch of calls to a given .py script
# python3 batch.py script_file txt_file


import sys
from subprocess import call

def main():

    script = sys.argv[1].strip()
    file = sys.argv[2].strip()

    print("Start batch of commands: \n")

    # iterate through folder
    with open(file,'r') as commands:
        next = commands.readline().split(' ')
        for i in range (len(next)):
            next[i] = next[i].strip("\n ")
        while (len(next) > 1): # check for EOF
            # run the command
            command = ['python3',script].extend(next)
            # call(command)
            print(command)
            next = commands.readline().strip().split(',')

    print("Finished batch.")


if __name__ == '__main__':
    main()
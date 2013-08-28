import sys
import argparse

if __name__ == "__main__":
    FILTER_STR = 'STATISTIC: '

    # add argument
    parser = argparse.ArgumentParser(description = "Parse YARN log for Hamster")
    parser.add_argument('-i')
    parser.add_argument('-d')
    parser.add_argument('-o')
    args = parser.parse_args(sys.argv[1:])

    if args.i == None or args.d == None or args.o == None:
        raise "must specify i/d/o"

    # open file
    ifs = open(args.i, 'r')
    try:
        ofs = open(args.o, 'a')
    except IOError:
        ofs = open(args.o, 'w')
    ofs.write('\n')
    ofs.write('==========================================\n')
    ofs.write(args.d + '\n')
    ofs.write('==========================================\n')
    line = ifs.readline()
    while len(line) > 0:
        if line.find(FILTER_STR) >= 0:
            out = line[line.find(FILTER_STR) + len(FILTER_STR): len(line)]
            ofs.write(out)
        line = ifs.readline()

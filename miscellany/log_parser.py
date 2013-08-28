import sys

if __name__ == "__main__":
    FILTER_STR = 'STATISTIC: '

    if len(sys.argv) != 4:
        raise IOError("using python log_parser.py outfile infile desc")

    # open file
    ifs = open(sys.argv[2], 'r')
    try:
        ofs = open(sys.argv[1], 'a')
    except IOError:
        ofs = open(sys.argv[1], 'w')
    ofs.write('\n')
    ofs.write('==========================================\n')
    ofs.write(sys.argv[3] + '\n')
    ofs.write('==========================================\n')
    line = ifs.readline()
    while len(line) > 0:
        if line.find(FILTER_STR) >= 0:
            out = line[line.find(FILTER_STR) + len(FILTER_STR): len(line)]
            print out[:len(out)-1]
            ofs.write(out)
        line = ifs.readline()

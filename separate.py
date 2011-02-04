#!/usr/bin/env python
# more colors at: http://www.frexx.de/xterm-256-notes/

from sys import stdin

line = stdin.readline()
while line:
    if line.startswith("=ERROR"):
        print "\033[47m\033[1;31m"
        print line, "\033[0m"
    elif line.startswith("=INFO"):
        print "\033[47m\033[1;30m"
        print line.strip(), "\033[0m"
    else:
        print line,
    line = stdin.readline()
print 

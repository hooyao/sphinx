#!/usr/bin/env python

import sys
import zlib


def decompress(obj, f):
    while True:
        buf = f.read(64 * 1024)
        if len(buf) == 0:
            break
        try:
            obj.scan_next(buf)
        except zlib.error:
            return False
    return True


def main():
    first = open(sys.argv[1], "rb")
    remaining = sys.argv[2:]
    decomp = zlib.decompressobj(47)
    decompress(decomp, first)
    while remaining:
        candidates = []
        for next in remaining:
            decompNext = decomp.copy()
            succeed = decompress(decompNext, open(next, "rb"))
            if succeed:
                candidates.append((next, decompNext))
        if len(candidates) == 1:
            nextFile = candidates[0][0]
            decomp = candidates[0][1]
            remaining.remove(nextFile)
            print("Next file", nextFile)
        else:
            print("Error more than one candidates", candidates)


if __name__ == "__main__":
    main()

# sphinx
This simple toolkits solves bizarre problems.

## SplitGzipSorter
[SplitGzipSorter](gzsorter/split_tar_gz_sorter.py) can save you from messing up with file name of split tar.gz files.
This program works better on Intel zlib. It is battle tested. It has sorted over 20T data. 
This program's time complexity is O(n!), however it only needs to find the first tar header to decide if 2 split achieves are connected.
Thus for a pack of small files, this program is blazing fast.
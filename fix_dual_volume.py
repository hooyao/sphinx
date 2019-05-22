#!/usr/bin/python3
import os
import re

IN_SOURCE_DIR = "~/fix_efapiao"
IN_DIST_DIR = "/xmldata/extract"
IN_DIST_DIR_1 = "/xmldata1/extract"
ANCHOR_STATS_FILE_NAME = 'xml_count_complete.txt'
ANCHOR_SUC_FILE_NAME = "extract_complete.txt"
STATS_FILE = "stats.csv"

DIR_NAME_PATTERN = re.compile(r"^(19[0-9]{2}|2[0-9]{3})(0[1-9]|1[012])([123]0|[012][1-9]|31)$")

source_dir = os.path.abspath(os.path.expanduser(IN_SOURCE_DIR))
dist_dir = os.path.abspath(os.path.expanduser(IN_DIST_DIR))

all_source_dirs = [f for f in os.listdir(source_dir)
                   if os.path.isdir(os.path.join(source_dir, f)) and DIR_NAME_PATTERN.match(f)]

for date_dir in sorted(all_source_dirs):
    anchor_file_list = [os.path.join(source_dir, date_dir, f)
                        for f in os.listdir(os.path.join(source_dir, date_dir)) if
                        os.path.isfile(os.path.join(source_dir, date_dir, f))
                        and ANCHOR_SUC_FILE_NAME in f]
    if len(anchor_file_list) > 0:
        if os.path.exists(os.path.join(source_dir, date_dir, "dist_path.txt")):
            os.remove(os.path.join(source_dir, date_dir, "dist_path.txt"))
        with open(os.path.join(source_dir, date_dir, "dist_path.txt"), "w") as f:
            print(f"writing {date_dir}")
            extracted_xml_path = os.path.join(dist_dir, date_dir)
            f.write(extracted_xml_path)

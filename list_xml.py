#!/usr/bin/python3
import glob
import logging
import os
import re
import time
from logging.handlers import WatchedFileHandler

import pandas as pd

IN_SOURCE_DIR = "~/fix_efapiao"
ANCHOR_STATS_FILE_NAME = 'xml_count_complete.txt'
ANCHOR_SUC_FILE_NAME = "extract_complete.txt"
STATS_FILE = "stats.csv"

DIR_NAME_PATTERN = re.compile(r"^(19[0-9]{2}|2[0-9]{3})(0[1-9]|1[012])([123]0|[012][1-9]|31)$")

formatter = logging.Formatter("%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s")

cwd = os.getcwd()
logg_file = os.path.join(cwd, "xml_stats.log")
file_handler = WatchedFileHandler(logg_file)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

main_logger = logging.getLogger("main")
main_logger.setLevel("INFO")

main_logger.addHandler(file_handler)
main_logger.addHandler(console_handler)

source_dir = os.path.abspath(os.path.expanduser(IN_SOURCE_DIR))

all_remaining_dirs = [(os.path.join(source_dir, f), f) for f in os.listdir(source_dir) if
                      os.path.isdir(os.path.join(source_dir, f)) and DIR_NAME_PATTERN.match(f)]

remaining = {p[1]: p for p in all_remaining_dirs}

while len(remaining) > 0:
    sorted_keys = sorted(remaining.keys())
    for key in sorted_keys:
        value = remaining[key]
        source_dir_path = value[0]
        dir_name = value[1]

        anchor_file_list = [f for f in os.listdir(source_dir_path) if
                            os.path.isfile(os.path.join(source_dir_path, f))
                            and (ANCHOR_STATS_FILE_NAME in f or ANCHOR_SUC_FILE_NAME in f)]

        if ANCHOR_SUC_FILE_NAME in anchor_file_list:
            if ANCHOR_STATS_FILE_NAME in anchor_file_list:
                # handled skip
                main_logger.info(f"{source_dir_path} is handled, skip.")
                del remaining[key]
            else:
                with open(os.path.join(source_dir_path, "dist_path.txt"), "r") as f:
                    dist_line = f.readline()

                dist_dir_path = os.path.abspath(os.path.expanduser(dist_line.strip()))
                if os.path.exists(dist_dir_path):
                    main_logger.info(f"counting {dist_dir_path}")

                    df = pd.DataFrame(columns=['xml'])

                    xml_file_paths = os.path.join(dist_dir_path, "*.xml")
                    rows = []
                    for xml_path in glob.iglob(xml_file_paths):
                        xml_file_name = os.path.split(xml_path)[-1]
                        row = {'xml': xml_file_name}
                        rows.append(row)
                    df = df.append(rows, ignore_index=False)
                    count = len(df)
                    df.to_csv(os.path.join(source_dir_path, STATS_FILE))
                    os.mknod(os.path.join(source_dir_path, ANCHOR_STATS_FILE_NAME))
                    del remaining[key]
                    main_logger.info(f"{dist_dir_path} has {count} xml")
    time.sleep(1)

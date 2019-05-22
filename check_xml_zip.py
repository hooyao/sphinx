#!/usr/bin/python3
import logging
import os
import re
import subprocess
from logging.handlers import WatchedFileHandler

IN_ARC_DIR = "/xmldata1/dedup/archive"
COMP_DEDUP_SUC_FILE = "compress_complete.txt"
COMP_CHECK_OK_FILE = "compress_check_ok.txt"
COMP_CHECK_BAD_FILE = "compress_check_bad.txt"

DIR_NAME_PATTERN = re.compile(r"^(19[0-9]{2}|2[0-9]{3})(0[1-9]|1[012])([123]0|[012][1-9]|31)$")

formatter = logging.Formatter("%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s")

cwd = os.getcwd()
logg_file = os.path.join(cwd, "compress_test.log")
file_handler = WatchedFileHandler(logg_file)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

main_logger = logging.getLogger("main")
main_logger.setLevel("INFO")

main_logger.addHandler(file_handler)
main_logger.addHandler(console_handler)

arc_dir = os.path.abspath(os.path.expanduser(IN_ARC_DIR))
if not os.path.exists(arc_dir):
    os.mkdir(arc_dir)

arc_dirs = [(os.path.join(arc_dir, f), f) for f in sorted(os.listdir(arc_dir)) if
            DIR_NAME_PATTERN.match(f) and os.path.isdir(os.path.join(arc_dir, f))]

for arc_dir in arc_dirs:
    arc_path = arc_dir[0]
    date_name = arc_dir[1]

    check_ok_file = [f for f in sorted(os.listdir(arc_path)) if
                     COMP_CHECK_OK_FILE in f
                     and os.path.isfile(os.path.join(arc_path, f))]
    if len(check_ok_file) > 0:
        main_logger.info(f"{date_name} is ok. skip.")
    else:
        file_pattern = os.path.join(arc_path, f"{date_name}.tar.gz.part0*")
        test_cmd = f"cat {file_pattern} |gzip -t"
        test_result = subprocess.run(test_cmd, shell=True, stderr=subprocess.PIPE)
        if test_result.returncode == 0:
            main_logger.info(f"{date_name} is ok.")
            os.mknod(os.path.join(arc_path, COMP_CHECK_OK_FILE))
        else:
            main_logger.error(f"{date_name} is corrupted. need rework.")
            os.mknod(os.path.join(arc_path, COMP_CHECK_BAD_FILE))

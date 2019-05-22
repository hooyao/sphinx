#!/usr/bin/python3
import json
import logging
import os
import re
from logging.handlers import WatchedFileHandler

IN_SOURCE_DIR = "~/fix_efapiao/"
IN_MAP_FILE = "~/fix_efapiao/mapping.json"
GZIP_PART_FILE_PREFIX = "sphinx_part"
SPHINX_FILE_NAME_PATTERN = re.compile(r"^sphinx_part_(\d{5})$")

DIR_NAME_PATTERN = re.compile(r"^(19[0-9]{2}|2[0-9]{3})(0[1-9]|1[012])([123]0|[012][1-9]|31)$")

formatter = logging.Formatter("%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s")

cwd = os.getcwd()
logg_file = os.path.join(cwd, "compress_dedup.log")
file_handler = WatchedFileHandler(logg_file)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

main_logger = logging.getLogger("main")
main_logger.setLevel("INFO")

main_logger.addHandler(file_handler)
main_logger.addHandler(console_handler)

source_dir = os.path.abspath(os.path.expanduser(IN_SOURCE_DIR))
map_file = os.path.abspath(os.path.expanduser(IN_MAP_FILE))

all_arc_dirs = [(os.path.join(source_dir, f), f) for f in sorted(os.listdir(source_dir)) if
                os.path.isdir(os.path.join(source_dir, f)) and DIR_NAME_PATTERN.match(f)]

mapping = {}
id_set = set()
for ele in all_arc_dirs:
    dir_path = ele[0]
    dir_name = ele[1]

    all_sphinx_file = [(os.path.join(dir_path, f), f) for f in sorted(os.listdir(dir_path)) if
                       os.path.isfile(os.path.join(dir_path, f)) and SPHINX_FILE_NAME_PATTERN.match(f)]

    ordered_list = {}

    for sphinx_file in all_sphinx_file:
        sphinx_file_path = sphinx_file[0]
        sphinx_file_name = sphinx_file[1]

        file_target = os.readlink(sphinx_file_path)
        target_id = os.path.split(file_target)[-1]

        sphinx_re = SPHINX_FILE_NAME_PATTERN.match(sphinx_file_name)
        order = int(sphinx_re.group(1))
        ordered_list[target_id] = order
        id_set.add(target_id)
    mapping[dir_name] = ordered_list

with open(map_file, "w", encoding="utf-8") as mapping_file:
    json.dump(obj=mapping, fp=mapping_file, ensure_ascii=False, indent=4)

print(f"file count: {len(id_set)}")

import os
import re

source_dir = os.path.abspath(os.path.expanduser("~/fix_efapiao"))
DIR_NAME_PATTERN = re.compile(r"^^(19[0-9]{2}|2[0-9]{3})(0[1-9]|1[012])([123]0|[012][1-9]|31)$")

all_dirs = [os.path.join(source_dir, f) for f in os.listdir(source_dir) if
            os.path.isdir(os.path.join(source_dir, f)) and DIR_NAME_PATTERN.match(f)]

result = []
for dir in all_dirs:
    anchor_file_list = [os.path.join(dir, f) for f in os.listdir(dir)
                        if os.path.isfile(os.path.join(dir, f))
                        and ("extract_complete.txt" in f or "extract_error.txt" in f)]
    if len(anchor_file_list) == 2:
        for file in anchor_file_list:
            if "extract_error.txt" in file:
                os.remove(file)

print(len(result))
print(result)

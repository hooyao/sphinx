#!/usr/bin/python3
import glob
import logging
import multiprocessing
import os
import re
import subprocess
import tarfile
import time
from copy import deepcopy
from logging.handlers import WatchedFileHandler


class Compressor:

    def process_xml(self, dir_name, dir_path):
        result = None
        main_logger.info(f"start processing {os.path.join(fix_efapiao_dir, dir_name)}")

        tar_file_path = os.path.join(arc_dir, dir_name, dir_name + ".tar.gz")
        xml_file_paths = os.path.join(dir_path, "*.xml")
        xml_count = 0
        try:
            if not os.path.exists(os.path.join(arc_dir, dir_name, COMP_DEDUP_ONGOING_FILE)):
                os.mknod(os.path.join(arc_dir, dir_name, COMP_DEDUP_ONGOING_FILE))
            with tarfile.open(name=tar_file_path, mode="w:gz", dereference=True) as tar:
                for xml_path in glob.iglob(xml_file_paths):
                    xml_file_name = os.path.split(xml_path)[-1]
                    tar.add(name=xml_path, arcname=xml_file_name)
                    xml_count += 1

            split_cmd = f"split -d -a 3 -b 1024m {tar_file_path} {tar_file_path}.part"
            split_result = subprocess.run(split_cmd, shell=True, stderr=subprocess.PIPE)
            if split_result.returncode == 0:
                main_logger.info(f"split {tar_file_path} completed.")
                par2_cmd = f"par2 create {tar_file_path}.part.par2 {tar_file_path}.part*"
                par2_result = subprocess.run(par2_cmd, shell=True, stderr=subprocess.PIPE)
                if par2_result.returncode == 0:
                    main_logger.info(f"par2 {tar_file_path}.part.par2 completed.")
                    with open(os.path.join(arc_dir, dir_name, COMP_DEDUP_SUC_FILE), "w") as f:
                        f.write(str(xml_count))
                    result = dir_name
                else:
                    main_logger.error(f"par2 {tar_file_path}.part.par2 failed.")
                    raise Exception(f"par2 {tar_file_path}.part.par2 failed.")
            else:
                main_logger.error(f"split {tar_file_path} failed.")
                raise Exception(f"split {tar_file_path} failed.")
        except Exception as e:
            main_logger.error(f"compressing {dir_name} failed, skip.", e)
        finally:
            if os.path.exists(os.path.join(arc_dir, dir_name, COMP_DEDUP_ONGOING_FILE)):
                os.remove(os.path.join(arc_dir, dir_name, COMP_DEDUP_ONGOING_FILE))
            if os.path.exists(tar_file_path):
                os.remove(tar_file_path)
        return result


IN_SOURCE_DIR = "/xmldata1/after_deduplicate"
IN_ARC_DIR = "/xmldata1/dedup/archive"
DEDUP_COMP_ANKOR = "deduplicate_complete.txt"
COMP_DEDUP_SUC_FILE = "compress_complete.txt"
COMP_DEDUP_ONGOING_FILE = "compress_ongoing.txt"

FIX_EFAPIAO_DIR = "~/fix_efapiao"

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
arc_dir = os.path.abspath(os.path.expanduser(IN_ARC_DIR))
if not os.path.exists(arc_dir):
    os.mkdir(arc_dir)
fix_efapiao_dir = os.path.abspath(os.path.expanduser(FIX_EFAPIAO_DIR))

all_remaining_dirs = [(os.path.join(source_dir, f), f) for f in os.listdir(source_dir) if
                      os.path.isdir(os.path.join(source_dir, f)) and DIR_NAME_PATTERN.match(f)]

remaining = {p[1]: p[0] for p in all_remaining_dirs}

async_task_ist = []
with multiprocessing.Pool(processes=4) as pool:
    while len(remaining) > 0:
        copy_remaining = deepcopy(remaining)
        sorted_keys = sorted(copy_remaining.keys())

        for key in sorted_keys:
            dir_path = copy_remaining[key]
            dir_name = key
            finish_dedup_ankor_file = os.path.join(fix_efapiao_dir, dir_name, DEDUP_COMP_ANKOR)
            if not os.path.exists(finish_dedup_ankor_file):
                # not ready
                continue
            else:
                need_process = True
                if os.path.exists(os.path.join(arc_dir, dir_name)):
                    complete_file = [f for f in sorted(os.listdir(os.path.join(arc_dir, dir_name))) if
                                     COMP_DEDUP_SUC_FILE in f
                                     and os.path.isfile(os.path.join(arc_dir, dir_name, f))]
                    ongoing_file = [f for f in sorted(os.listdir(os.path.join(arc_dir, dir_name))) if
                                    COMP_DEDUP_ONGOING_FILE in f
                                    and os.path.isfile(os.path.join(arc_dir, dir_name, f))]

                    if len(complete_file) > 0:
                        main_logger.info(f"{dir_name} is completed, skip.")
                        del copy_remaining[key]
                        continue
                    if len(ongoing_file) > 0:
                        main_logger.info(f"{dir_name} is being processed, skip.")
                        continue
                else:
                    os.mkdir(os.path.join(arc_dir, dir_name))

                compressor = Compressor()
                task = pool.apply_async(func=compressor.process_xml, args=(dir_name, dir_path))
                async_task_ist.append(task)
        remaining = copy_remaining
        time.sleep(5)
    for task in async_task_ist:
        task.wait()

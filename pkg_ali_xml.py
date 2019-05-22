import logging
import os
import subprocess
import tarfile
from logging.handlers import WatchedFileHandler

import pandas as pd

formatter = logging.Formatter("%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s")
cwd = os.getcwd()
logg_file = os.path.join(cwd, "ali_pkg_dedup.log")
file_handler = WatchedFileHandler(logg_file)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

main_logger = logging.getLogger("main")
main_logger.setLevel("INFO")

main_logger.addHandler(file_handler)
main_logger.addHandler(console_handler)

IDX_FILE_DIR = "/root/ali_dedup/"

idx_file_dir = os.path.abspath(os.path.expanduser(IDX_FILE_DIR))

idx_files = [(os.path.join(idx_file_dir, f), f) for f in os.listdir(idx_file_dir)]

xml_source_dirs = ['/data/output/success/20160601',
                   '/data/output/success/201605',
                   '/data/output/success/20151130_20160601',
                   '/data/output/failed']

archive_dir = os.path.abspath(os.path.expanduser("/data/ali_dedup_archive"))

for idx_file in idx_files:
    date_str = idx_file[1].split('.')[0]
    df = pd.read_csv(idx_file[0], sep="\n", header=None)
    df.columns = ["file_name"]

    tar_file_path = os.path.join(archive_dir, f"{date_str}.tar.gz")
    with tarfile.open(name=tar_file_path,
                      mode="w:gz") as tar:
        max_not_found = len(xml_source_dirs)
        for index, row in df.iterrows():
            xml_file_name = row[0]
            if index % 10000 == 0:
                progress = index / len(df)
                main_logger.info(f"progress for {date_str}:{progress:.3f}")

            file_found = False
            for possible_dir in xml_source_dirs:
                try:
                    xml_file_path = possible_dir + os.sep + xml_file_name
                    tar.add(name=xml_file_path, arcname=xml_file_name)
                    file_found = True
                    break
                except Exception as e:
                    pass
            if not file_found:
                main_logger.error(f"{xml_file_name} is not found in {possible_dir}")

    split_cmd = f"split -d -a 3 -b 1024m {tar_file_path} {tar_file_path}.part"
    split_result = subprocess.run(split_cmd, shell=True, stderr=subprocess.PIPE)
    if split_result.returncode == 0:
        main_logger.info(f"split {tar_file_path} completed.")
        par2_cmd = f"par2 create {tar_file_path}.part.par2 {tar_file_path}.part0*"
        par2_result = subprocess.run(par2_cmd, shell=True, stderr=subprocess.PIPE)
        if par2_result.returncode == 0:
            main_logger.info(f"par2 {tar_file_path}.part.par2 completed.")
            os.remove(tar_file_path)
            os.mknod(os.path.join(archive_dir, f"{date_str}.complete"))
        else:
            main_logger.error(f"par2 {tar_file_path}.part.par2 failed.")
    else:
        main_logger.error(f"split {tar_file_path} failed.")

main_logger.info("archiving is complete.")

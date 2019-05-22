#!/usr/bin/python3
import argparse
import copy
import io
import json
import logging
import math
import multiprocessing
import os
import struct
import tarfile
import time
import zlib
from itertools import repeat
from logging.handlers import WatchedFileHandler

file_name_offset = 0
file_name_len = 100

size_offset = 124
size_len = 12

checksum_offset = 148
checksum_len = 8

_GZIP_PART_FILE_PREFIX = "sphinx_part"
_ANUBIS_PART_FILE_PREFIX = "anubis_part"


class Sphinx:
    GZ_HEADER_MAGIC = b'\x1f\x8b\x08'

    DEFAULT_TRUNK_SIZE = 128 * 1024
    TAR_HEAD_SIZE = 512
    TAR_BLOCK_SIZE = 512

    TAR_TAIL_MARK = "tail"
    TAR_BODY_MARK = "body"
    TAR_HEAD_MARK = "head"

    total_read_in_bytes = 0

    def __init__(self, target_dir, logger):
        self.target_dir = target_dir
        self.lookup = {}
        self.logger = logger
        # self.total_read_in_bytes = 0

    def find_head(self, path_list):
        for file_path in path_list:
            with open(file_path, 'rb') as f:
                a = f.read(3)
                if self.GZ_HEADER_MAGIC == a:
                    # TODO we assume only on head in every single directory
                    return file_path
        return None

    def scan_head(self, head_path):
        self.logger.info("start processing head")
        decomp_obj = zlib.decompressobj(47)
        is_single_head = True
        with open(head_path, "rb") as head_file:
            data = bytearray()
            while True:
                buf = head_file.read(self.DEFAULT_TRUNK_SIZE)
                self.total_read_in_bytes += self.DEFAULT_TRUNK_SIZE
                if len(buf) == 0:
                    break
                try:
                    data.extend(decomp_obj.decompress(buf))
                except zlib.error:
                    # self.lookup[lookup_key] = False
                    # return False
                    raise Exception("The head is corrupted. Can't proceed.")
            tar_file_object = io.BytesIO(data)
            with tarfile.open(fileobj=tar_file_object) as tar:
                last_file = None
                try:
                    reach_end = False
                    while not reach_end:
                        last_file = tar.next()
                        if last_file is None:
                            reach_end = True
                except tarfile.ReadError:
                    # reach the end of head but there is still more data
                    is_single_head = False
                if last_file is not None:
                    header_offset = last_file.offset
                    last_file_data = data[header_offset:]
                else:
                    last_file_data = None
                return is_single_head, last_file_data, 0, decomp_obj, self.TAR_HEAD_MARK

    @staticmethod
    def parse_tar_header(header_data):
        header = dict()
        tar_header = struct.unpack('=100s8s8s8s12s12s8sc100s6s2s32s32s8s8s155s12x', header_data)
        header['name'] = tar_header[0]
        header['mode'] = tar_header[1]
        header['uid'] = tar_header[2]
        header['gid'] = tar_header[3]
        header['size'] = tarfile.nti(tar_header[4])
        header['mtime'] = tar_header[5]
        header['chksum'] = tarfile.nti(tar_header[6])
        header['typeflag'] = tar_header[7]
        header['linkname'] = tar_header[8]
        header['magic'] = tar_header[9]
        header['version'] = tar_header[10]
        header['uname'] = tar_header[11]
        header['gname'] = tar_header[12]
        header['devmajor'] = tar_header[13]
        header['devminor'] = tar_header[14]
        header['prefix'] = tar_header[15]

        if header['chksum'] not in tarfile.calc_chksums(header_data):
            raise tarfile.InvalidHeaderError("chksum not match")
        return header

    @staticmethod
    def parse_tar_header_simple(header_data):
        header = dict()

        header['size'] = tarfile.nti(header_data[size_offset:size_offset + size_len])

        header['chksum'] = tarfile.nti(header_data[checksum_offset:checksum_offset + checksum_len])

        if header['chksum'] not in tarfile.calc_chksums(header_data):
            raise tarfile.InvalidHeaderError("chksum not match")
        return header

    def scan_next(self, last_scan_result, current_path, next_path):
        lookup_key = "{current_path}->{next_path}".format(current_path=current_path, next_path=next_path)
        # print(f"testing {lookup_key}")
        conti = self.lookup.get(lookup_key, True)
        if not conti:
            return False, None, None, None, None
        else:
            is_single_header = last_scan_result[0]
            last_file_data = copy.deepcopy(last_scan_result[1])
            data_to_skip = copy.deepcopy(last_scan_result[2])
            decomp_obj = last_scan_result[3].copy()

            data = last_file_data
            total_read = 0
            with open(next_path, "rb") as f:
                try:
                    while True:
                        decomp_buf = bytearray()
                        buf = f.read(self.DEFAULT_TRUNK_SIZE)
                        self.total_read_in_bytes += self.DEFAULT_TRUNK_SIZE
                        total_read += self.DEFAULT_TRUNK_SIZE
                        if len(buf) == 0:
                            # reach the file end
                            return True, data, data_to_skip, decomp_obj, self.TAR_BODY_MARK
                        try:
                            decomp_buf = decomp_obj.decompress(buf)
                        except zlib.error:
                            # self.lookup[lookup_key] = False
                            # return False
                            raise zlib.error("GZ decompress fail.")
                        data.extend(decomp_buf)
                        if data_to_skip > len(data):
                            continue
                        elif data_to_skip > 0:
                            data = data[data_to_skip:]
                        reach_end = False
                        while not reach_end:
                            if len(data) >= self.TAR_HEAD_SIZE:
                                header = self.parse_tar_header_simple(data[0:self.TAR_HEAD_SIZE])

                                pl_size = math.ceil(header['size'] / self.TAR_BLOCK_SIZE) * self.TAR_BLOCK_SIZE
                                tar_entry_size = self.TAR_HEAD_SIZE + pl_size
                                if tar_entry_size < len(data):
                                    data = data[tar_entry_size:]
                                else:
                                    data_to_skip = tar_entry_size - len(data)
                                    data = bytearray()
                                    reach_end = True
                            else:
                                data_to_skip = 0
                                reach_end = True
                except tarfile.InvalidHeaderError as err:
                    if not any(data):
                        self.logger.info("reaches the last file")
                        return True, bytearray(), 0, decomp_obj, self.TAR_TAIL_MARK
                    else:
                        # print("look like this file is not continuous")
                        # print(err)
                        return False, None, -1, None, None
                except zlib.error as zerr:
                    # print("GZ decompress error")
                    return False, None, -1, None, None

    def sort(self):
        sorted_result = []
        all_paths = [os.path.join(self.target_dir, f) for f in os.listdir(self.target_dir) if
                     os.path.isfile(os.path.join(self.target_dir, f))]
        head = self.find_head(all_paths)
        if head:
            remaining = {p: p for p in all_paths}
            del remaining[head]
            head_scan_result = self.scan_head(head)

            # self.scan_next(decomp, "-1", head)
            if head_scan_result[0]:  # if only head
                sorted_result = [(head, self.TAR_TAIL_MARK)]
            else:
                sorted_result = [(head, head_scan_result[4])] + self.recursive_sort(head_scan_result, head, remaining)
        else:
            raise Exception("There is no GZ head in this directory {target_dir}".format(target_dir=self.target_dir))
        return sorted_result

    def recursive_sort(self, last_scan_result, current_path, remaining):
        total_read_in_m = self.total_read_in_bytes / 1024 / 1024
        self.logger.info("total read: {total_read_in_m} M".format(total_read_in_m=total_read_in_m))
        candidate_results = []
        if len(remaining) > 0:
            candidates = []
            for next_path in remaining:
                scan_result = self.scan_next(last_scan_result, current_path, next_path)
                success = scan_result[0]
                if success:
                    candidates.append((next_path, scan_result))
            if len(candidates) > 0:
                for candidate in candidates:
                    next_part_path = candidate[0]
                    scan_result = candidate[1]
                    if scan_result[4] == self.TAR_TAIL_MARK:
                        candidate_results.append([(next_part_path, scan_result[4])])
                    else:
                        remaining_temp = copy.deepcopy(remaining)
                        del remaining_temp[next_part_path]
                        tmp = self.recursive_sort(scan_result, next_part_path, remaining_temp)
                        tmp2 = [(next_part_path, scan_result[4])] + tmp
                        candidate_results.append(tmp2)
            if len(candidate_results) > 0:
                candidate_results = max(candidate_results, key=len)
        return candidate_results


def parse_args():
    parser = argparse.ArgumentParser(prog='Efapiao gzip parts fix program')
    parser.add_argument('-i', '--inventory', type=str, required=True,
                        help='The inventory of efapiao gzip part detail.')
    parser.add_argument('-d', '--dir', type=str, required=True,
                        help='The directory where efapiao gzip parts exist.')
    parser.add_argument('-ds', '--date_start', type=str, required=True,
                        help='The start date of efapiao gzip parts to be considered.')
    parser.add_argument('-de', '--date_end', type=str, required=True,
                        help='The end date of efapiao gzip parts to be considered.')
    return parser.parse_args()


def setup_logger_handler(start, end):
    formatter = logging.Formatter("%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s")

    cwd = os.getcwd()
    logg_file = os.path.join(cwd, f"{start}-{end}.log")
    file_handler = WatchedFileHandler(logg_file)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    return file_handler, console_handler


def process_for_one_day(item, main_logger, sphinx_logger, gzip_parts_dir):
    cwd = os.getcwd()
    fix_output_dir = os.path.join(cwd, item[0])
    if not os.path.exists(fix_output_dir):
        os.makedirs(fix_output_dir)

    anchor_file_list = [os.path.join(fix_output_dir, f) for f in os.listdir(fix_output_dir) if
                        os.path.isfile(os.path.join(fix_output_dir, f)) and f.split(".")[-1] == "txt"]
    if len(anchor_file_list) > 0:
        main_logger.info(f'{fix_output_dir} is already handled, skip...')
    else:
        v = sorted(item[1], key=lambda x: x['OriDate'])
        for i in range(len(v)):
            src = os.path.join(gzip_parts_dir, v[i]['ArchiveId'])
            dst = os.path.join(fix_output_dir, _ANUBIS_PART_FILE_PREFIX + '_' + str(i).zfill(5))
            if os.path.exists(src):
                main_logger.info(f'Rename {src} to {dst}.')
                if os.path.exists(dst):
                    os.remove(dst)
                os.symlink(src, dst)
            else:
                main_logger.info(f'{src} does not exist, skip renaming.')
        start = time.time()
        sphinx = Sphinx(fix_output_dir, sphinx_logger)
        sorted_result = sphinx.sort()
        end = time.time()
        for i in range(len(sorted_result)):
            src = sorted_result[i][0]
            dst = os.path.join(fix_output_dir, _GZIP_PART_FILE_PREFIX + '_' + str(i).zfill(5))
            os.rename(src, dst)
        if len(sorted_result) > 0:
            if sorted_result[-1][1] == Sphinx.TAR_TAIL_MARK:
                os.mknod(os.path.join(fix_output_dir, "sphinx_complete.txt"))
                main_logger.info(f'Sorting parts in {fix_output_dir} is complete.')
            else:
                os.mknod(os.path.join(fix_output_dir, "sphinx_half.txt"))
                main_logger.info(f'Sorting parts in {fix_output_dir} is incomplete or corrupted.')
        main_logger.info(f'Sorting parts in {fix_output_dir} is done.')
        main_logger.info(f"Total Read: {sphinx.total_read_in_bytes/1024/1024} M")
        read_speed_m = sphinx.total_read_in_bytes / 1024 / 1024 / (end - start)
        main_logger.info(f"speed {read_speed_m} M/s")


def main():
    args = parse_args()

    inventory_file_path = args.inventory
    gzip_parts_dir = args.dir
    date_start = args.date_start
    date_end = args.date_end

    handlers = setup_logger_handler(date_start, date_end)
    main_logger = logging.getLogger("main_logger")
    main_logger.setLevel("INFO")
    for handler in handlers:
        main_logger.addHandler(handler)

    sphinx_logger = logging.getLogger("sphinx_log")
    sphinx_logger.setLevel("INFO")
    for handler in handlers:
        sphinx_logger.addHandler(handler)

    inventory = {}
    main_logger.info('Initializing...')
    with open(inventory_file_path, "r", encoding='utf-8') as f:
        data = json.load(f)
        for item in data:
            key = item['OriDate']
            if key:
                if date_start <= key <= date_end:
                    arr = inventory.get(key, [])
                    if not arr:
                        inventory[key] = arr
                    arr.append(item)
            else:
                archive_id = item['ArchiveId']
                main_logger.info(f'Warning: {archive_id} has empty OriDate')
    if inventory:
        with multiprocessing.Pool(processes=4) as pool:
            results = pool.starmap(func=process_for_one_day,
                                   iterable=zip(inventory.items(), repeat(main_logger), repeat(sphinx_logger),
                                                repeat(gzip_parts_dir)),
                                   chunksize=1)
    else:
        main_logger.info(f'No gzip parts found in date range: {date_start}...{date_end}, exit.')


if __name__ == '__main__':
    main()

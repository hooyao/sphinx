#!/usr/bin/env python
"""
@author:hooyao
@license: Apache Licence
@file: split_tar_gz_sorter.py
@time: 2019/05/22
@contact: hooyao@gmail.com
@site:
@software: PyCharm
"""

import copy
import io
import math
import os
import struct
import tarfile
import zlib

# Important field offset and length in tar header
file_name_offset = 0
file_name_len = 100

size_offset = 124
size_len = 12

checksum_offset = 148
checksum_len = 8


class SplitTarGzSorter:
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
        header = {'size': tarfile.nti(header_data[size_offset:size_offset + size_len]),
                  'chksum': tarfile.nti(header_data[checksum_offset:checksum_offset + checksum_len])}

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

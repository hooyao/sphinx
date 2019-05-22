#!/usr/bin/python3
import argparse
import copy
import json
import os
import subprocess
import zlib


class Sphinx:
    _GZ_HEADER_MAGIC = b'\x1f\x8b\x08'
    _GZIP_PART_FILE_PREFIX = 'sphinx_part'
    TRUNK_SIZE = 64 * 1024

    def __init__(self, target_dir):
        self.target_dir = target_dir
        self.lookup = {}
        self.total_read_in_bytes = 0

    def decompress(self, obj, current_path, next_path):
        lookup_key = f"{current_path}->{next_path}"
        # print(f"testing {lookup_key}")
        if lookup_key in self.lookup:
            return self.lookup.get(lookup_key)
        else:
            with open(next_path, "rb") as f:
                #print(f"[Sphinx] processing {lookup_key}")
                while True:
                    buf = f.read(self.TRUNK_SIZE)
                    self.total_read_in_bytes = self.total_read_in_bytes + len(buf)
                    if len(buf) == 0:
                        break
                    try:
                        obj.decompress(buf)
                    except zlib.error:
                        self.lookup[lookup_key] = False
                        return False
                self.lookup[lookup_key] = True
                return True

    def find_head(self, path_list):
        for file_path in path_list:
            with open(file_path, 'rb') as f:
                a = f.read(3)
                if self._GZ_HEADER_MAGIC == a:
                    # TODO we assume only on head in every single directory
                    return file_path
        return None

    def recursive_sort(self, decomp, current_path, remaining):
        candidate_results = []
        if len(remaining) > 0:
            candidates = []
            for next_path in remaining:
                decomp_next = decomp.copy()
                success = self.decompress(decomp_next, current_path, next_path)
                if success:
                    candidates.append((next_path, decomp_next, decomp_next.eof))
            if len(candidates) > 0:
                print("[Sphinx]" + current_path + " has condidates:" + str(candidates))
                for candidate in candidates:
                    if candidate[2] is True:
                        candidate_results.append([(candidate[0], "tail")])
                    else:
                        next_part_path = candidate[0]
                        decomp_temp = candidate[1]
                        remaining_temp = copy.deepcopy(remaining)
                        del remaining_temp[next_part_path]
                        tmp = self.recursive_sort(decomp_temp, next_part_path, remaining_temp)
                        tmp2 = [(next_part_path, "body")] + tmp
                        candidate_results.append(tmp2)
            if len(candidate_results) > 0:
                candidate_results = max(candidate_results, key=len)
        #print("[Sphinx]" + str(candidate_results))
        return candidate_results

    def sort(self):
        print(f'[Sphinx] Sorting parts in {self.target_dir} starts...')
        sorted_result = []
        all_paths = [os.path.join(self.target_dir, f) for f in os.listdir(self.target_dir) if
                     os.path.isfile(os.path.join(self.target_dir, f))]
        head = self.find_head(all_paths)
        if head:
            sorted_result.append((head, "head"))
            remaining = {p: p for p in all_paths}
            del remaining[head]
            decomp = zlib.decompressobj(47)
            self.decompress(decomp, "-1", head)
            sorted_result = sorted_result + self.recursive_sort(decomp, head, remaining)
            for i in range(len(sorted_result)):
                src = sorted_result[i][0]
                dst = os.path.join(self.target_dir, self._GZIP_PART_FILE_PREFIX + '_' + str(i).zfill(5))
                os.rename(src, dst)
            if len(sorted_result) > 0:
                if sorted_result[-1][1] == "tail":
                    os.mknod(os.path.join(self.target_dir, "sphinx_complete.txt"))
                else:
                    os.mknod(os.path.join(self.target_dir, "sphinx_half.txt"))
            print(f'[Sphinx] Sorting parts in {self.target_dir} is done.')
            print(f"[Sphinx] Total Read: {self.total_read_in_bytes/1024/1024} M")
        else:
            print(f'[Sphinx] There is no GZ head in this directory {self.target_dir}, stop sorting.')
            # raise Exception(f"[Sphinx] There is no GZ head in this directory {self.target_dir}")
            os.mknod(os.path.join(self.target_dir, "sphinx_no_head.txt"))


class Anubis:
    _GZIP_PART_FILE_PREFIX = 'anubis_part'

    def __init__(self, inventory, target_dir):
        self.inventory = inventory
        self.target_dir = target_dir

    def _run_gzip_test(self, path):
        print(f'[Anubis] Run `gzip -t` to verify parts in {path}.')
        cmd = [f'cat {path}/{self._GZIP_PART_FILE_PREFIX}* | gzip -t']
        result = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE)
        return result.stderr

    def run(self):
        for (k, v) in self.inventory.items():
            cwd = os.getcwd()
            fix_output_dir = os.path.join(cwd, k)
            if not os.path.exists(fix_output_dir):
                os.makedirs(fix_output_dir)

            anchor_file_list = [os.path.join(fix_output_dir, f) for f in os.listdir(fix_output_dir) if
                                os.path.isfile(os.path.join(fix_output_dir, f)) and f.split(".")[-1] == "txt"]
            if len(anchor_file_list) > 0:
                print(f'{fix_output_dir} is already handled, skip...')
            else:
                v = sorted(v, key=lambda x: x['OriDate'])
                for i in range(len(v)):
                    src = os.path.join(self.target_dir, v[i]['ArchiveId'])
                    dst = os.path.join(fix_output_dir, self._GZIP_PART_FILE_PREFIX + '_' + str(i).zfill(5))
                    if os.path.exists(src):
                        print(f'[Anubis] Rename {src} to {dst}.')
                        if os.path.exists(dst):
                            os.remove(dst)
                        os.symlink(src, dst)
                    else:
                        print(f'[Anubis] {src} does not exist, skip renaming.')

                res = self._run_gzip_test(fix_output_dir)
                # res = True
                if not res:
                    print(f'[Anubis] Verified parts in {fix_output_dir} are complete and ordered.')
                    os.mknod(os.path.join(fix_output_dir, "anubis_complete.txt"))
                else:
                    print(f'[Anubis] Verified parts in {fix_output_dir} are incomplete or unordered.')
                    os.mknod(os.path.join(fix_output_dir, "anubis_failed.txt"))
                    # print(f'[Anubis] Hand over to Sphinx to proceed...')
                    # Sphinx(fix_output_dir).sort()


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


def main():
    args = parse_args()

    inventory_file_path = args.inventory
    gzip_parts_dir = args.dir
    date_start = args.date_start
    date_end = args.date_end

    inventory = {}
    print('Initializing...')
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
                print(f'Warning: {archive_id} has empty OriDate')
    if inventory:
        print('[Anubis] Fixing starts...')
        Anubis(inventory, gzip_parts_dir).run()
        print('[Anubis] Fixing ends.')
    else:
        print(f'No gzip parts found in date range: {date_start}...{date_end}, exit.')


if __name__ == '__main__':
    main()
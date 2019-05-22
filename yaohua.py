import copy
import json
import os
import subprocess
import sys
import zlib


class Sphinx:
    GZ_HEADER_MAGIC = b'\x1f\x8b\x08'
    _GZIP_PART_FILE_PREFIX = 'sphinx_part'

    def __init__(self, target_dir):
        self.target_dir = target_dir
        self.lookup = {}

    def decompress(self, obj, current_path, next_path):
        lookup_key = current_path + '->' + next_path
        # print(f"testing {lookup_key}")
        conti = self.lookup.get(lookup_key,True)
        if not conti:
            return False
        else:
            with open(next_path, "rb") as f:
                while True:
                    buf = f.read(64 * 1024)
                    if len(buf) == 0:
                        break
                    try:
                        obj.scan_next(buf)
                    except zlib.error:
                        self.lookup[lookup_key] = False
                        return False
                return True

    def find_head(self, path_list):
        for file_path in path_list:
            with open(file_path, 'rb') as f:
                a = f.read(3)
                if self.GZ_HEADER_MAGIC == a:
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
                    candidates.append((next_path, decomp_next))
            if len(candidates) > 0:
                for candidate in candidates:
                    next_part_path = candidate[0]
                    decomp_temp = candidate[1]
                    remaining_temp = copy.deepcopy(remaining)
                    del remaining_temp[next_part_path]
                    tmp = self.recursive_sort(decomp_temp, next_part_path, remaining_temp)
                    tmp2 = [next_part_path] + tmp
                    if tmp2 is None:
                        print("stop here")
                    candidate_results.append(tmp2)
            if len(candidate_results) > 0:
                candidate_results = max(candidate_results, key=len)
        # print(candidate_results)
        return candidate_results

    def sort(self):
        sorted_result = []
        all_paths = [os.path.join(self.target_dir, f) for f in os.listdir(self.target_dir) if
                     os.path.isfile(os.path.join(self.target_dir, f))]
        head = self.find_head(all_paths)
        if head:
            sorted_result.append(head)
            remaining = {p: p for p in all_paths}
            del remaining[head]
            decomp = zlib.decompressobj(47)
            self.decompress(decomp, "-1", head)
            sorted_result = [head] + self.recursive_sort(decomp, head, remaining)
        else:
            raise Exception(f"There is no GZ head in this directory {self.target_dir}")

        for i in range(len(sorted_result)):
            src = sorted_result[i]
            dst = os.path.join(self.target_dir, self._GZIP_PART_FILE_PREFIX + '_' + str(i).zfill(5))
            os.rename(src, dst)


class Anubis:
    _GZIP_PART_FILE_PREFIX = 'anubis_part'

    def __init__(self, inventory, target_dir):
        self.inventory = inventory
        self.target_dir = target_dir

    def _is_valid_part(self, part):
        if part.startswith('.'):
            return False
        return os.path.isfile(os.path.join(self.target_dir, part))

    def _run_gzip_test(self, path):
        cmd = [f'cat {path}/{self._GZIP_PART_FILE_PREFIX}* | gzip -t']
        result = subprocess.run(cmd, shell=True, stderr=subprocess.PIPE)
        return result.stderr

    def run(self):
        for (k,v) in self.inventory.items():
            fix_output_dir = os.path.join(self.target_dir, k)
            if not os.path.exists(fix_output_dir):
                os.makedirs(fix_output_dir)

            v = sorted(v, key=lambda x: x['OriDate'])
            for i in range(len(v)):
                src = os.path.join(self.target_dir, v[i]['ArchiveId'])
                dst = os.path.join(fix_output_dir, self._GZIP_PART_FILE_PREFIX + '_' + str(i).zfill(5))
                os.rename(src, dst)
            res = self._run_gzip_test(fix_output_dir)
            if not res:
                print(f'Anubis verified, parts in {fix_output_dir} are in correct order.')
            else:
                print(f'Anubis verified, parts in {fix_output_dir} are in wrong order, let Sphinx proceed...')
                Sphinx(fix_output_dir).sort()


def main(argv):
    inventory_path = argv[1]
    target_dir = argv[2]

    inventory = {}
    with open(inventory_path, "r", encoding='utf-8') as f:
        data = json.load(f)
        for item in data:
            key = item['OriDate']
            if key:
                if key == '20161001':
                    arr = inventory.get(key, [])
                    if not arr:
                        inventory[key] = arr
                    arr.append(item)
            else:
                archive_id = item['ArchiveId']
                # print(f'{archive_id} has empty OriDate')
    Anubis(inventory, target_dir).run()


if __name__ == '__main__':
    main(sys.argv)
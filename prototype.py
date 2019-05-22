#!/usr/bin/python3
import copy
import os
import sys
import zlib


class Sphinx:
    GZ_HEADER_MAGIC = b'\x1f\x8b\x08'
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
                print(f"[Sphinx] processing {lookup_key}")
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
                if self.GZ_HEADER_MAGIC == a:
                    # TODO we assume only on head in every single directory
                    return file_path
        return None

    def sort(self):
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
        else:
            raise Exception(f"There is no GZ head in this directory {self.target_dir}")
        return sorted_result

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
        print("[Sphinx]" + str(candidate_results))
        return candidate_results


def main(*args):
    sphinx = Sphinx("./50M")
    sorted_result = sphinx.sort()
    print(sorted_result)
    print(f"total read: {sphinx.total_read_in_bytes/1024/1024} M")


if __name__ == "__main__":
    main(*sys.argv[1:])

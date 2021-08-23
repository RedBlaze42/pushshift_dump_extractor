import json, time
import zstandard as zstd
from tqdm import tqdm
import os, glob

def treat_files(comment_method, slug):
    for file_path in glob.glob(slug):
        print(file_path)
        treat_file(comment_method, file_path)

def human_size(bytes, units=[' bytes','KB','MB','GB','TB', 'PB', 'EB']):
    bytes = int(bytes)
    return str(bytes) + units[0] if bytes < 1024 else human_size(bytes>>10, units[1:])

def human_b10(bytes, units=['','K','M','G','T', 'P', 'E']):
    return "{:.2f}{}".format(float(bytes), units[0]) if bytes < 1000 else human_b10(bytes / 1000, units[1:])

def treat_file(comment_method, file_name):
    last_update = 0
    chunk_nb = 0
    comment_count = 0
    start = time.time()
    with open(file_name, 'rb') as fh:
        dctx = zstd.ZstdDecompressor(max_window_size = 2**31)
        with dctx.stream_reader(fh) as reader:
            previous_line = ""
            with tqdm(total = os.path.getsize(file_name), mininterval = 0.5, unit='B', unit_scale=True, unit_divisor=1024) as pbar:
                while True:
                    chunk = reader.read(2**24)
                    chunk_nb += 1

                    update = int(fh.tell())
                    pbar.update(update - last_update)
                    pbar.set_postfix({"count":human_b10(comment_count),"size":human_size(chunk_nb*(2**24))})
                    last_update = update
                    
                    if not chunk:
                        break

                    string_data = chunk.decode('utf-8')
                    lines = string_data.split("\n")
                    for i, line in enumerate(lines[:-1]):
                        if i == 0:
                            line = previous_line + line

                        comment = json.loads(line)
                        
                        comment_method(comment)
                        comment_count += 1
                        
                    previous_line = lines[-1]
    size_treated, time_elapsed = chunk_nb*(2**24), int(time.time() - start)
    print("Finished treating {} or {} comments, {}/s Lines: {}/s".format(human_size(size_treated), human_b10(comment_count), human_size(size_treated/time_elapsed), human_b10(comment_count/time_elapsed)))
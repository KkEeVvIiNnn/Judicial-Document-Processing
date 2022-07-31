import os
import re
import json
import argparse
import time

from multiprocessing import Process, JoinableQueue, Lock
from selectolax.parser import HTMLParser

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", type=str, help="The path/directory of the input file(s).")
parser.add_argument("-o", "--output", type=str, help="The directory of the output file(s).")
args = parser.parse_args()

def get_text_selectolax(html):
    if html:
        tree = HTMLParser(html)
    
        if tree.body is None:
            return None
    
        for tag in tree.css('script'):
            tag.decompose()
        for tag in tree.css('style'):
            tag.decompose()
    
        text = tree.body.text(separator='\n')
        return text
    else:
        return None

def process_line(work_id, queue, outfile, lock):
    count = 0
    ofn = open(outfile, "a", encoding='utf-8')
    while True:
        try:
            line = queue.get()
            data = json.loads(line)
            new_instance = {}

            for key, value in data.items():
                if key != "html":
                    new_instance[key] = value
                else:
                    new_instance['content'] = get_text_selectolax(value)

            lock.acquire()
            ofn.write(json.dumps(new_instance, ensure_ascii=False)+'\n')
            lock.release()
            count += 1
            if count % 10000 == 0:
                print(work_id, count)
        finally:
            queue.task_done()

def producer(queue:JoinableQueue, infile):
    ifn = open(infile, "r", encoding='utf-8')
    for line in ifn:
        queue.put(line)

def process(infile, outfile):

    write_lock = Lock()

    queue = JoinableQueue(100)

    pc = Process(target=producer, args=(queue, infile))
    f = open(outfile, "w")
    f.close()
    pc.start()

    workercount = 18
    for i in range(workercount):
        worker = Process(target=process_line, args=(i, queue, outfile, write_lock))
        worker.daemon = True
        worker.start()
    pc.join()
    queue.join()

def load_files(file_path):
    """
        function: load files from the file_path
        args: file_path  -- a directory or a specific file
    """
    if os.path.isfile(file_path):
        fns = [file_path]
    elif os.path.isdir(file_path):
        file_dir = file_path
        fns = [os.path.join(file_dir, fn) for fn in os.listdir(file_dir)]
    else:
        print("No such file or directory: ", file_path)
        fns = []
    # print("file path: ", fns)
    return fns

if __name__ == '__main__':
    files = load_files(args.input)
    os.makedirs(args.output, exist_ok=True)

    for filepath in files:
        infile = filepath
        outfile = os.path.join(args.output, "parsed_"+os.path.split(infile)[-1])
        process(infile, outfile)
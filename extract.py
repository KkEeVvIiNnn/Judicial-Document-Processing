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

def judge(data):
    ret = [0, 0, 0, 0, 0]
    for key, value in data.items():
        if isinstance(value, str) and ("《中华人民共和国民法典》" in value or "《民法典》" in value):
            ret[0] = 1
        if key == "cause" and isinstance(value, str):
            if "民间借贷纠纷" in value or "作品信息网络传播权纠纷" in value:
                ret[1] = 1
            if ("知识产权" in value or "著作权" in value or "专利权" in value or "商标权" in value) and ("侵犯" in value or "侵权" in value):
                ret[2] = 1
            if "受贿" in value:
                ret[3] = 1
            if "帮助信息网络犯罪活动" in value:
                ret[4] = 1

    return ret

def process_line(work_id, queue, outpath, lock):
    count = 0
    of1 = open(os.path.join(outpath, "q1.txt"), "a", encoding='utf-8')
    of2 = open(os.path.join(outpath, "q2.txt"), "a", encoding='utf-8')
    of3 = open(os.path.join(outpath, "q3.txt"), "a", encoding='utf-8')
    of4 = open(os.path.join(outpath, "q4.txt"), "a", encoding='utf-8')
    of5 = open(os.path.join(outpath, "q5.txt"), "a", encoding='utf-8')
    while True:
        try:
            line = queue.get()
            data = json.loads(line)

            ret = judge(data)
            if ret[0]:
                lock.acquire()
                of1.write(json.dumps(data, ensure_ascii=False)+'\n')
                lock.release()
            if ret[1]:
                lock.acquire()
                of2.write(json.dumps(data, ensure_ascii=False)+'\n')
                lock.release()
            if ret[2]:
                lock.acquire()
                of3.write(json.dumps(data, ensure_ascii=False)+'\n')
                lock.release()
            if ret[3]:
                lock.acquire()
                of4.write(json.dumps(data, ensure_ascii=False)+'\n')
                lock.release()
            if ret[4]:
                lock.acquire()
                of5.write(json.dumps(data, ensure_ascii=False)+'\n')
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

def process(infile, outpath):

    write_lock = Lock()

    queue = JoinableQueue(100)

    pc = Process(target=producer, args=(queue, infile))
    pc.start()

    workercount = 18
    for i in range(workercount):
        worker = Process(target=process_line, args=(i, queue, outpath, write_lock))
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
    f = open(os.path.join(args.output, "q1.txt"), "w")
    f.close()
    f = open(os.path.join(args.output, "q2.txt"), "w")
    f.close()
    f = open(os.path.join(args.output, "q3.txt"), "w")
    f.close()
    f = open(os.path.join(args.output, "q4.txt"), "w")
    f.close()
    f = open(os.path.join(args.output, "q5.txt"), "w")
    f.close()

    for filepath in files:
        infile = filepath
        outpath = args.output
        process(infile, outpath)
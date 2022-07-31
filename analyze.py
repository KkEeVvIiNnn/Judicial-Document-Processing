import os
import re
import json
import argparse
import time

from multiprocessing import Process, JoinableQueue, Lock

key_map = [
    ("id", "id"),
    ("doc_id", "doc_id"),
    ("browse_count", "浏览"),
    ("publish_date", "pub_date"),
    ("upload_date", "上传日期"),
    ("trial_date", "裁判日期"),
    # ("court_heard", "审理法院"),
    ("trail_member", "审判人员"),
    ("lawyer", "律师"),
    ("law_firm", "律所"),
    ("court_name", "法院名称"),
    ("court_id", "法院ID"),
    ("court_province", "法院省份"),
    ("court_city", "法院地市"),
    ("court_region", "法院区域"),
    ("court_district", "法院区县"),
    ("effect_level", "效力层级"),
    ("pub_prosecution_org", "公诉机关"),
    ("admin_behavior_type", "行政行为种类"),
    ("admin_manage_scope", "行政管理范围"),
    ("case_name", "案件名称"),
    ("case_id", "案号"),
    ("case_type", "案件类型"),
    ("appellor", "当事人"),
    ("settle_type", "结案方式"),
    ("cause", "案由"),
    ("trial_round", "审理程序"),
    ("doc_type", "文书类型"),
    ("fulltext_type", "文书全文类型"),
    ("basics_text", "案件基本情况段原文"),
    ("judge_record_text", "诉讼记录段原文"),
    ("head_text", "文本首部段落原文"),
    ("tail_text", "文本尾部原文"),
    ("judge_result_text", "判决结果段原文"),
    ("judge_member_text", "诉讼参与人信息部分原文"),
    ("abbr_adjudication_text", "裁判要旨段原文"),
    ("case_skb_text", "诉控辩原文"),
    ("judge_reson_text", "理由原文"),
    ("additional_text", "附加原文"),
    ("correction_text", "补正文书"),
    ("private_reason", "不公开理由"),
    ("legal_base", "legal_base"),
    ("doc_content", "DocContent"),
    ("status", "status"),
    ("etl", "etl"),
    ("html", "html"),
    ("d", "d"),
    ("channel", "channel"),
    ("crawl_time", "crawl_time"),
    ("is_private", "公开类型"),
    ("keywords", "关键字")
]

new_key = {
    "s1":"案件名称",
    "s2":"法院名称",
    "s4":"效力层级",
    "s6":"文书类型",
    "s7":"案号",
    "s8":"案件类型",
    "s9":"审理程序",
    "s10":"审理程序",
    "s11":"案由",
    "s12":"案由",
    "s13":"案由",
    "s14":"案由",
    "s15":"案由",
    "s16":"案由",
    "s17":"当事人",
    "s18":"审判人员",
    "s19":"律师",
    "s20":"律所",
    "s21":"附加原文",
    "s22":"文本首部段落原文",
    "s23":"诉讼记录段原文",
    "s24":"诉控辩原文",
    "s25":"案件基本情况段原文",
    "s26":"理由原文",
    "s27":"判决结果段原文",
    "s28":"文本尾部原文",
    "s29":"legal_base",
    "s31":"裁判日期",
    "s32":"不公开理由",
    "s33":"法院省份",
    "s34":"法院地市",
    "s35":"法院区县",
    "s41":"pub_date",
    "s43":"公开类型",
    "s45":"关键字",
    "s46":"结案方式",
    "s47":"legal_base",
    "flyj":"legal_base",
    "cprq":"裁判日期",
    "qwContent": "html",
    "viewCount": "浏览",
    "Title": "案件名称",
    "PubDate": "pub_date",
    "Html": "html"
}

re_number = re.compile(r'\d+')

def extract_number(str):
    ret = ''.join(re_number.findall(str))
    try:
        return int(ret)
    except:
        return 0

def convert_date(str):
    number = ''.join(re_number.findall(str))
    try:
        number = int(number)
        return time.strftime("%Y-%m-%d", time.localtime(number/1000))
    except:
        return None

def convert_leagal_base(x):
    if not x:
        return None
    ret = []
    for regulation in x:
        for key in list(regulation.keys()):
            if key != "Items" and key != "法规名称":
                regulation["法规名称"] = regulation[key]
        regulation_name = regulation["法规名称"]
        converted_regulation = {
            "regulation_name": regulation_name,
            "Items": []
        }
        articles = regulation["Items"]
        for article in articles:
            for key in list(article.keys()):
                if key != "法条名称" and key != "法条内容":
                    if "名" in key or "称" in key:
                        article["法条名称"] = article[key]
                    if "内" in key or "容" in key:
                        article["法条内容"] = article[key]
            converted_article = {
                "article_name": article["法条名称"],
                "article_content": article["法条内容"]
            }
            converted_regulation['Items'].append(converted_article)
        ret.append(converted_regulation)
    return ret

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

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input", type=str, help="The path/directory of the input file(s).")
parser.add_argument("-o", "--output", type=str, help="The directory of the output file(s).")
args = parser.parse_args()

def extract_type1(str):
    ret = []
    key = ""
    value = ""
    in_q = False
    quotation_type = ""
    has_comma = False
    i = 0
    max_l = len(str)
    while i < max_l:
        c = str[i]
        if c == "\"":
            if in_q == False:
                in_q = True
                quotation_type = c
                key = ""
                value = ""
                has_comma = False
            elif in_q == True and quotation_type == c:
                in_q = False
                if has_comma:
                    if key == "浏览":
                        ret.append([key, value])
                        return ret
            else:
                if in_q:
                    if has_comma:
                        value += c
                    else:
                        key += c
        elif c == "\\":
            i += 1
            if i < max_l and in_q and has_comma:
                value += str[i]
            if i < max_l and in_q and not has_comma:
                key += str[i]
        elif c == ":" or c == "：":
            if in_q:
                has_comma = True
        else:
            if in_q:
                if has_comma:
                    value += c
                else:
                    key += c
        i += 1
    return ret

def extract_type2(str):
    ret = []
    key = ""
    value = ""
    in_q = False
    quotation_type = ""
    has_comma = False
    i = 0
    max_l = len(str)
    while i < max_l:
        c = str[i]
        if c == "\"" or c == "\'":
            if in_q == False:
                in_q = True
                quotation_type = c
            elif in_q == True and quotation_type == c:
                in_q = False
                if has_comma:
                    ret.append([key, value])
                    key = ""
                    value = ""
                    has_comma = False
            else:
                if in_q:
                    if has_comma:
                        value += c
                    else:
                        key += c
        elif c == "\\":
            i += 1
            if i < max_l and in_q and has_comma:
                value += str[i]
            if i < max_l and in_q and not has_comma:
                key += str[i]
        elif c == ":" or c == "：":
            if in_q:
                if has_comma:
                    value += c
                else:
                    key += c
            else:
                if key != "":
                    has_comma = True
        else:
            if in_q:
                if has_comma:
                    value += c
                else:
                    key += c
            else:
                if c not in [' \t\r\n']:
                    key = ""
                    value = ""
                    has_comma = False
        i += 1
    return ret

re_null = re.compile(r'"([^"]*?)":\s*null')
re_triple = re.compile(r'name:\s*"([^"]*?)"\s*,\s*key:\s*"([^"]*?)"\s*,\s*value:\s*"([^"]*?)"')
re_pub = re.compile(r'\\\"PubDate\\\":\\\"([^"]*?)\\\"')
re_html = re.compile(r'\\\"Html\\\":\\\"([^"]*?)\\\"')
def extract_type3(str):
    ret = []
    for key in re_null.findall(str):
        ret.append([key, None])
    for key in re_triple.findall(str):
        ret.append([key[0], key[2]])
    for key in re_pub.findall(str):
        ret.append(['pub_date', key])
    for key in re_html.findall(str):
        ret.append(['html', key])
    return ret

def add_quote(matched):
    value = matched.group(2)
    if value != "Items" and "法" not in value and "名称" not in value and "内容" not in value:
        return matched.group()
    if "法规名" in value:
        value = "法规名称"
    if "规名称" in value:
        value = "法规名称"
    if "法条内" in value:
        value = "法条内容"
    if "条内容" in value:
        value = "法条内容"
    if "法条名" in value:
        value = "法条名称"
    if "条名称" in value:
        value = "法条名称"
    return matched.group(1) + '"' + value + '":'
def switch_quote(matched):
    return matched.group().replace("'", "\"")

re_switch_q = re.compile(r"[:,\{\}\[\]']{2}")
re_add_q = re.compile(r"(\{|,)([^\{,]*?):")
def extract_type4(str):
    i = str.find("LegalBase:")
    if i < 0:
        return None
    max_l = len(str)
    depth = 0
    in_q = False
    raw_text = ""
    while i < max_l:
        c = str[i]
        if c == '[' and depth == 0:
            in_q = True
        if c == '[':
            depth += 1
        if c == ']':
            depth -= 1
        if in_q:
            raw_text += c
        if in_q and depth == 0:
            break
        i += 1
    
    if in_q:
        txt = raw_text
        # raw_text = raw_text.replace("'", "\"")
        raw_text = re_switch_q.sub(switch_quote, raw_text)
        # raw_text = raw_text.replace("法规名称", "\"法规名称\"")
        # raw_text = raw_text.replace("Items", "\"Items\"")
        # raw_text = raw_text.replace("法条名称", "\"法条名称\"")
        # raw_text = raw_text.replace("法条内容", "\"法条内容\"")
        raw_text = re_add_q.sub(add_quote, raw_text)
        try:
            return json.loads(raw_text)
        except:
            print(txt)
            raise
    else:
        return None

def decode_source(str):
    ret = {}
    str = str.replace("&nbsp;", "")
    str = str.replace("\u3000", " ")
    try:
        # "xx:xx"
        pair1 = extract_type1(str)
        for key, value in pair1:
            if key != "{\"Title\"":
                ret[key] = value

        # "xx":"xx"
        pair2 = extract_type2(str)
        for key, value in pair2:
            ret[key] = value

        # "xx":xx
        pair3 = extract_type3(str)
        for key, value in pair3:
            ret[key] = value

        # legalbase
        legalbase = extract_type4(str)
        ret['legal_base'] = legalbase
    finally:
        return ret

def decode_source_new(str):
    ret = {}
    try:
        data = json.loads(str)
        for key, value in data.items():
            if key == "s17" or key == "s45" or key == "s11" or key == "s12" or key == "s13" or key == "s14" or key == "s15" or key == "s16":
                try:
                    value = ','.join(value)
                except:
                    value = ""
            if key in new_key and new_key[key] not in ret:
                ret[new_key[key]] = value
    finally:
        return ret

def process_line(work_id, queue, outfile, lock):
    count = 0
    ofn = open(outfile, "a", encoding='utf-8')
    while True:
        try:
            line = queue.get()
            line = ''.join(line.split('\t')[1:])
            data = json.loads(line)
            instance = {}
            for key, value in data.items():
                if key != 'source':
                    instance[key] = value
                else:
                    if value:
                        if value[0] == '{':
                            new_data = decode_source_new(value)
                        else:
                            new_data = decode_source(value) 
                        for new_key, new_value in new_data.items():
                            instance[new_key] = new_value

                
                new_instance = {}
                for (key1, key2) in key_map:
                    if key2 in instance:
                        if key1 == 'browse_count':
                            value = instance[key2]
                            new_instance[key1] = extract_number(value)
                        elif key1 == 'upload_date':
                            value = instance[key2]
                            new_instance[key1] = convert_date(value)
                        elif key1 == "legal_base":
                            value = instance[key2]
                            new_instance[key1] = convert_leagal_base(value)
                        else:
                            new_instance[key1] = instance[key2]
                    else:
                        new_instance[key1] = None

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
        

if __name__ == '__main__':
    files = load_files(args.input)
    os.makedirs(args.output, exist_ok=True)

    for filepath in files:
        infile = filepath
        outfile = os.path.join(args.output, "processed_"+os.path.split(infile)[-1])
        process(infile, outfile)
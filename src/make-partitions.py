#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
from sys import stderr
from json import loads
from time import time, localtime, strftime
from urllib import urlopen, quote
from argparse import ArgumentParser
from subprocess import check_call, check_output

from DE_memory_publicFun import loadConf as load_conf

def log(message):

    stderr.write(message + "\n")
    stderr.flush()
    
def alert(message):
    
    #urlopen("http://alarms.ops.qihoo.net:8360/intfs/alarm_intf?group_name=new_recommend&subject=%s&content=%s" %
    #        (quote(message.decode("utf-8").encode("gbk")), quote("如题".decode("utf-8").encode("gbk")))).read()
    print "alert: ", message

def determin_partition(input_dir, output_dir):

    check_call(["mkdir", "-p", input_dir])
    output = check_output(["ls", input_dir])
    splited_output = output.strip().split()
    if len(splited_output) == 0:
        message = u"partition目录下无内容, dir: %s, output: %s" % (input_dir, output)
        log(message)
        alert(message)
        return None, None, None, None, None, None
    
    reverse_timestamp_set = set([])
    forward_timestamp_set = set([])
    for file in splited_output:
        if file.endswith(".url.tag.nrot.done"):
            timestamp = file.split(".")[0]
            reverse_timestamp_set.add(timestamp)
        if file.endswith(".url.info.done"):
            timestamp = file.split(".")[0]
            forward_timestamp_set.add(timestamp)
    if len(forward_timestamp_set & reverse_timestamp_set) == 0:
        message = u"partition目录下没有至少一个有效的完整的partition, dir: %s, output: %s" % (input_dir, output)
        log(message)
        alert(message)
        return None, None, None, None, None, None
    timestamp = sorted(list(forward_timestamp_set & reverse_timestamp_set), reverse = True)[0]

    forward_input_file = "%s/%s.url.info" % (input_dir, timestamp)
    reverse_input_file = "%s/%s.url.tag.nrot" % (input_dir, timestamp)
    forward_output_file = "%s/%s.e" % (output_dir, timestamp)
    reverse_output_file = "%s/%s.d" % (output_dir, timestamp)
    reverse_origin_file = "%s/%s.n" % (output_dir, timestamp)
    return forward_input_file, reverse_input_file, forward_output_file, reverse_output_file, reverse_origin_file, timestamp

def consume_partition(forward_input_file, reverse_input_file, forward_output_file, reverse_output_file, reverse_origin_file, bucket_name, timestamp):

    log("正排输入文件 %s" % forward_input_file)
    log("倒排输入文件 %s" % reverse_input_file)
    with open(forward_input_file, 'r') as fd:
        for line in fd:
            splited_line = line.strip().split("\t")
            if len(splited_line) != 2:
                message = u"正排输入数据列数不对, bucket: %s, filename: %s, 这一行有%d个列: %s" % (len(splited_line), forward_input_file, bucket_name, line.rstrip("\n").decode("utf-8"))
                log(message)
                alert(message)
                assert False
            url, info_str = splited_line
            info_dict = loads(info_str)
            for key in ["source", "ntag", "ori_url"]:
                if "source" not in info_dict:
                    message = u"正排输入数据中缺少\"%s\"字段, bucket: %s, filename: %s, url: %s" % (key, bucket_name, forward_input_file, url)
                    log(message)
                    alert(message)
                    assert False
                if ";" in info_dict["source"]:
                    message = u"正排输入\"source\"字段中包含\";\", bucket: %s, filename: %s, url: %s" % (bucket_name, forward_input_file, url)
                    log(message)
                    alert(message)
                    assert False
                if "," in info_dict["ntag"]:
                    message = u"正排输入\"ntag\"字段中包含\",\", bucket: %s, filename: %s, url: %s" % (bucket_name, forward_input_file, url)
                    log(message)
                    alert(message)
                    assert False

    reverse_index_dict = {} # tag -> {"url": score, "url": score ...}
    with open(reverse_input_file, 'r') as fd:
        for line in fd:
            splited_line = line.strip().split("\t")
            if len(splited_line) != 3:
                message = u"倒排输入数据列数不对, bucket: %s, filename: %s, 这一行有%d个列: %s" % (bucket_name, reverse_input_file, len(splited_line), line.rstrip("\n").decode("utf-8"))
                log(message)
                alert(message)
                assert False
            url, tag, score = splited_line
            if bucket_name:
                if not tag.endswith("_%s" % (bucket_name)):
                    message = u"倒排tag的后缀不对, bucket: %s, filename: %s, tag: %s" % (bucket_name, reverse_input_file, tag.decode("utf-8"))
                    log(message)
                    alert(message)
                    assert False
            score = float(score)
            
            if tag not in reverse_index_dict:
                reverse_index_dict[tag] = {}
            if url in reverse_index_dict[tag] and score != reverse_index_dict[tag][url]:
                message = u"倒排数据下相同tag内url的nrot记录不一致, bucket: %s, filename: %s, tag: %s, url: %s, 原nort: %f, 现nrot %f" % (bucket_name, reverse_input_file, tag.decode("utf-8"), url, reverse_index_dict[tag][url], score)
                log(message)
                alert(message)
                assert False
            reverse_index_dict[tag][url] = score
            
    with open(reverse_output_file, 'w') as fd:
        for tag, url_dict in reverse_index_dict.iteritems():
            if len(url_dict) == 0:
                continue
            sorted_list = sorted([(url, score) for url, score in url_dict.iteritems()],
                                 key = lambda term: term[1], reverse = True)
            sorted_str = "\001".join([url for url, score in sorted_list[:200]])
            fd.write("%s\t%s\n" % (tag, sorted_str))

    check_call(["cp", "-f", forward_input_file, forward_output_file])
    check_call(["cp", "-f", reverse_input_file, reverse_origin_file])

    check_call(["touch", "%s.done" % reverse_output_file])
    check_call(["touch", "%s.done" % forward_output_file])
    check_call(["touch", "%s.done" % reverse_origin_file])

def main():

    parser = ArgumentParser()
    parser.add_argument("conf_file", help = "configure file")
    args = parser.parse_args()
    
    conf_file = args.conf_file
    
    conf_dict = load_conf(conf_file)
    for name in conf_dict:
        if name in ["merger_dir", "upload_dir"]:
            continue
        for key in ["input", "parse_output", "nrot_input", "DP_output"]:
            if key not in conf_dict[name]:
                message = u"conf_dict里%s缺少%s字段" % (name, key)
                log(message)
                alert(message)
                assert False

        check_call(["mkdir", "-p", conf_dict[name]["DP_output"]])
                
        assert name.startswith("test_")
    
        bucket_name = None
        if re.search("_V[0-9]+$", name):
            bucket_name = name.split("_")[-1]
    
        input_dir = conf_dict[name]["nrot_input"]
        output_dir = conf_dict[name]["DP_output"]
    
        if bucket_name:
            assert input_dir.endswith(bucket_name)
            assert output_dir.endswith(bucket_name)
        else:
            assert not re.search("_V[0-9]+$", input_dir)
            assert not re.search("_V[0-9]+$", output_dir)
        
        try:
            forward_input_file, reverse_input_file, forward_output_file, reverse_output_file, reverse_origin_file, timestamp = determin_partition(input_dir, output_dir)
            if None in [forward_input_file, reverse_input_file, forward_output_file, reverse_output_file, reverse_origin_file]:
                continue
            consume_partition(forward_input_file, reverse_input_file, forward_output_file, reverse_output_file, reverse_origin_file, bucket_name, timestamp)
        except Exception, e:
            message = "make-partition: [name %s]\n" % str(name) + str(e)
            log(message)
            alert(message)
            if name == "test_main":
                raise e

if __name__ == "__main__":

    main()


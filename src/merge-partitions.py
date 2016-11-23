#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re

from sys import stderr
from os.path import isfile
from json import loads, dumps
from time import time, strftime, localtime
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

def determin_partition(partition_dir):

    check_call(["mkdir", "-p", partition_dir])
    output = check_output(["ls", partition_dir])
    splited_output = output.strip().split()
    if len(splited_output) == 0:
        return None

    timestamp_set = set([])
    for file in splited_output:
        if file.endswith(".e"):
            timestamp = file.split(".")[0]
            timestamp_set.add(timestamp)
        if file.endswith(".n"):
            timestamp = file.split(".")[0]
            timestamp_set.add(timestamp)
        if file.endswith(".d"):
            timestamp = file.split(".")[0]
            timestamp_set.add(timestamp)

    for timestamp in sorted(timestamp_set, reverse = True):
        e_file = "%s/%s.e" % (partition_dir.rstrip("/"), timestamp)
        d_file = "%s/%s.d" % (partition_dir.rstrip("/"), timestamp)
        n_file = "%s/%s.n" % (partition_dir.rstrip("/"), timestamp)
        e_done = "%s/%s.e.done" % (partition_dir.rstrip("/"), timestamp)
        d_done = "%s/%s.d.done" % (partition_dir.rstrip("/"), timestamp)
        n_done = "%s/%s.n.done" % (partition_dir.rstrip("/"), timestamp)
        if not isfile(e_file):
            continue
        if not isfile(d_file):
            continue
        if not isfile(n_file):
            continue
        if not isfile(e_done):
            continue
        if not isfile(d_done):
            continue
        if not isfile(n_done):
            continue
        return timestamp
    return None
    
def move_partition(partition_dir, merger_dir, name):

    timestamp = determin_partition(partition_dir)
    if timestamp is None:
        message = u"合并实验时发现目录%s没有完整的一组数据" % partition_dir.decode("utf-8")
        log(message)
        return
    
    source_e_file = "%s/%s.e" % (partition_dir.rstrip("/"), timestamp)
    source_d_file = "%s/%s.d" % (partition_dir.rstrip("/"), timestamp)
    source_n_file = "%s/%s.n" % (partition_dir.rstrip("/"), timestamp)

    target_e_file = "%s/%s.e" % (merger_dir, name)
    target_d_file = "%s/%s.d" % (merger_dir, name)
    target_n_file = "%s/%s.n" % (merger_dir, name)
    target_done_file = "%s/%s.done" % (merger_dir, name)

    check_call(["cp", "-f", source_e_file, target_e_file])
    check_call(["cp", "-f", source_d_file, target_d_file])
    check_call(["cp", "-f", source_n_file, target_n_file])
    check_call(["touch", target_done_file])

def merge_files(source_path_set, target_path):

    with open(target_path, 'w') as out_fd:
        for source_path in source_path_set:
            with open(source_path, 'r') as in_fd:
                for line in in_fd:
                    out_fd.write("%s" % line)

def merge_source_value(value_one, value_two):

    return ",".join(set(value_one.split(",")) | set(value_two.split(",")))

def merge_ntag_value(value_one, value_two):

    return ";".join(set(value_one.split(";")) | set(value_two.split(";")))

def merge_info_dict(info_dict_one, info_dict_two):

    for key, value in info_dict_two.iteritems():
        if key in info_dict_one:
            if key is "source":
                info_dict_one[key] = merge_source_value(info_dict_one[key], value)
            if key is "ntag":
                info_dict_one[key] = merge_ntag_value(info_dict_one[key], value)
        else:
            info_dict_one[key] = value
    return info_dict_one

def merge_forward_index(source_path_list, target_path):

    url_info_dict = {}
    for source_path in source_path_list:
        with open(source_path, 'r') as fd:
            for line in fd:
                splited_line = line.rstrip("\n").split("\t")
                if len(splited_line) != 2:
                    message = u"合并倒排格式错误, 这一行有%d列, 行: %s" % (len(splited_line), line)
                    log(message)
                    alert(message)
                    assert False
                url, info_str = splited_line
                info_dict = loads(info_str)
                if url in url_info_dict:
                    info_dict = merge_info_dict(url_info_dict[url], info_dict)
                url_info_dict[url] = info_dict

    with open(target_path, 'w') as fd:
        for url, info_dict in url_info_dict.iteritems():
            fd.write("%s\t%s\n" % (url, dumps(info_dict)))

def merge_partitions(merger_dir, upload_dir, name_set):

    output = check_output(["ls", merger_dir])
    splited_output = output.strip().split()
    if len(splited_output) == 0:
        return

    for name in list(name_set):
        if not isfile("%s/%s.done" % (merger_dir, name.lstrip("test_"))):
            message = u"实验%s缺少done文件, 导致无法参与合并上传, merger_dir: %s" % (name.lstrip("test_"), merger_dir)
            log(message)
            alert(message)
            name_set.remove(name)
            continue
        if not isfile("%s/%s.n" % (merger_dir, name.lstrip("test_"))):
            message = u"实验%s缺少n文件, 导致无法参与合并上传, merger_dir: %s" % (name.lstrip("test_"), merger_dir)
            log(message)
            alert(message)
            name_set.remove(name)
            continue
        if not isfile("%s/%s.e" % (merger_dir, name.lstrip("test_"))):
            message = u"实验%s缺少e文件, 导致无法参与合并上传, merger_dir: %s" % (name.lstrip("test_"), merger_dir)
            log(message)
            alert(message)
            name_set.remove(name)
            continue
        if not isfile("%s/%s.d" % (merger_dir, name.lstrip("test_"))):
            message = u"实验%s缺少d文件, 导致无法参与合并上传, merger_dir: %s" % (name.lstrip("test_"), merger_dir)
            log(message)
            alert(message)
            name_set.remove(name)
            continue

    d_file_set = set([])
    e_file_set = set([])
    n_file_set = set([])
    for name in name_set:
        if isfile("%s/%s.done" % (merger_dir, name.lstrip("test_"))):
            d_file_set.add("%s.d" % name.lstrip("test_"))
            e_file_set.add("%s.e" % name.lstrip("test_"))
            n_file_set.add("%s.n" % name.lstrip("test_"))

    timestamp = strftime("%Y%m%d%H%M%S", localtime(time()))
    d_file_path = "%s/%s.d" % (upload_dir.rstrip("/"), timestamp)
    e_file_path = "%s/%s.e" % (upload_dir.rstrip("/"), timestamp)
    n_file_path = "%s/%s.n" % (upload_dir.rstrip("/"), timestamp)

    tag_set = set([])
    for d_file in d_file_set:
        reverse_index_file = "%s/%s" % (merger_dir.rstrip("/"), d_file)
        with open(reverse_index_file, 'r') as fd:
            for line in fd:
                splited_line = line.rstrip("\n").split("\t")
                tag, _ = splited_line
                if tag in tag_set:
                    message = u"tag在倒排中有重复, filename: %s, tag: %s" % (reverse_index_file, tag.decode("utf-8"))
                    log(message)
                    alert(message)
                    assert False
                tag_set.add(tag)

    tag_url_dict = {}
    for n_file in n_file_set:
        nrot_file = "%s/%s" % (merger_dir.rstrip("/"), n_file)
        with open(nrot_file, 'r') as fd:
            for line in fd:
                splited_line = line.rstrip("\n").split("\t")
                url, tag, nrot = splited_line
                if tag in tag_url_dict and url in tag_url_dict[tag] and tag_url_dict[tag][url] != nrot:
                    message = u"相同(url, tag)在nrot文件中有冲突, filename: %s, url: %s, tag: %s, nrot: %s" % (nrot_file, url, tag.decode("utf-8"), nrot)
                    log(message)
                    alert(message)
                    assert False
                if tag not in tag_url_dict:
                    tag_url_dict[tag] = {}
                if url not in tag_url_dict[tag]:
                    tag_url_dict[tag][url] = nrot
                if float(nrot) == 0.0:
                    message = u"tag在nrot文件中有nrot值为0, filename: %s, url: %s, tag: %s, nrot: %s" % (nrot_file, url, tag.decode("utf-8"), nrot)
                    log(message)
                tag_set.add(tag)

    #check_call(["rm", "-rf", upload_dir])
    check_call(["mkdir", "-p", upload_dir])
    merge_forward_index(["%s/%s" % (merger_dir.rstrip("/"), e_file) for e_file in list(e_file_set)], e_file_path) # merge forward index
    merge_files(["%s/%s" % (merger_dir.rstrip("/"), d_file) for d_file in list(d_file_set)], d_file_path) # merge reverse index
    merge_files(["%s/%s" % (merger_dir.rstrip("/"), n_file) for n_file in list(n_file_set)], n_file_path) # merge url info
    check_call(["touch"] + ["%s.done" % d_file_path])
    check_call(["touch"] + ["%s.done" % e_file_path])
    check_call(["touch"] + ["%s.done" % n_file_path])

def main():

    parser = ArgumentParser()
    parser.add_argument("conf_file", help = "configure file")
    args = parser.parse_args()
    
    conf_file = args.conf_file
    conf_dict = load_conf(conf_file)

    assert "merger_dir" in conf_dict
    assert "upload_dir" in conf_dict
    assert "merger_dir" in conf_dict["merger_dir"]
    assert "upload_dir" in conf_dict["upload_dir"]

    merger_dir = conf_dict["merger_dir"]["merger_dir"]
    upload_dir = conf_dict["upload_dir"]["upload_dir"]
    check_call(["mkdir", "-p", merger_dir])
    check_call(["mkdir", "-p", upload_dir])

    partition_dir_set = set([])
    name_set = set([])
    for name in conf_dict:
        if name in ["merger_dir", "upload_dir"]:
            continue
        for key in ["input", "parse_output", "nrot_input", "DP_output"]:
            if key not in conf_dict[name]:
                message = u"conf_dict里%s缺少%s字段" % (name, key)
                log(message)
                alert(message)
                assert False
                
        assert name.startswith("test_")
    
        name_set.add(name)
        partition_dir = conf_dict[name]["DP_output"]
        check_call(["mkdir", "-p", partition_dir])
        partition_dir_set.add(partition_dir)
        try:
            move_partition(partition_dir, merger_dir, "_".join(name.split("_")[1:]))
        except Exception, e:
            message = "merge-partition: [name %s]\n" % name + str(e)
            log(message)
            alert(message)
            if name == "test_main":
                raise e

    merge_partitions(merger_dir, upload_dir, name_set)

if __name__ == "__main__":

    main()

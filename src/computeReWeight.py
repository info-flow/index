#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sys import stderr
from argparse import ArgumentParser

def log(message):

    stderr.write(message.encode("utf-8") + "\n")
    stderr.flush()
    
def alert(message):
    
    #urlopen("http://alarms.ops.qihoo.net:8360/intfs/alarm_intf?group_name=new_recommend&subject=%s&content=%s" %
    #        (quote(message.decode("utf-8").encode("gbk")), quote("如题".decode("utf-8").encode("gbk")))).read()
    print "alert: ", message.encode("utf-8")

def main():

    parser = ArgumentParser()
    parser.add_argument("weight_file", help = "weight file (input)")
    parser.add_argument("nrot_file", help = "nrot file (input)")
    parser.add_argument("output_file", help = "output file (output)")
    args = parser.parse_args()

    nrot_file = args.nrot_file
    weight_file = args.weight_file
    output_file = args.output_file

    tag_weight_dict = {}
    with open(weight_file, 'r') as fd:
        for line in fd:
            splited_line = line.strip().split("\t")
            if len(splited_line) < 2:
                message = u"通知建锋, quality.current.score文件里有一条记录列数不对, 列数应至少为2, 而这一行列数为%d, line: %s" % (len(splited_line), line.rstrip("\n").decode("utf-8"))
                log(message)
                alert(message)
                continue
            tag = splited_line[0]
            weight = splited_line[1]
            try:
                weight = float(weight)
            except Exception, e:
                message = u"通知建锋, quality.current.score文件里有一条记录的第二列无法解析成float, weight: %s, line: %s" % (weight.decode("utf-8"), line.rstrip("\n").decode("utf-8"))
                log(message)
                alert(message)
                continue
            if tag in tag_weight_dict:
                message = u"通知建锋, quality.current.score文件里有一条tag重复, tag: %s, line: %s" % (tag.decode("utf-8"), line.rstrip("\n").decode("utf-8"))
                log(message)
                alert(message)
                continue
            tag_weight_dict[tag] = float(weight)
                
    with open(nrot_file, 'r') as input_fd:
        with open(output_file, 'w') as output_fd:
            for line in input_fd:
                splited_line = line.rstrip("\n").split("\t")
                if len(splited_line) != 3:
                    message = u"通知建民, nrot文件里有一条记录列数不对, 列数应为3, 而这一行列数为%d, line: %d" % (len(splited_line), line.rstrip("\n").decode("utf-8"))
                    log(message)
                    alert(message)
                    continue
                url, tag, nrot = splited_line
                try:
                    nrot = float(nrot)
                except Exception, e:
                    message = u"通知建民, nrot文件里有一条记录的第三列无法解析成float, nrot: %s, line: %s" % (nrot.decode("utf-8"), line.rstrip("\n").decode("utf-8"))
                    log(message)
                    alert(message)
                    continue
                if tag in tag_weight_dict:
                    nrot *= weight
                output_fd.write("%s\t%s\t%f\n" % (url, tag, nrot))

                
if __name__ == "__main__":

    main()

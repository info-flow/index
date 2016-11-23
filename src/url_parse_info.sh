#!/bin/sh
## 处理用户输入url的文件，输出拆解为url+tag 及 url+url_dict 的服务

PYTHON=/da1/db/python27/bin/python
now=$(date +%Y%m%d)

conf_file=./conf
kws_file=../conf/topic.word.20161020
$PYTHON  url_parse_info.py $conf_file $kws_file 1>> ../log/url_parse_info.log.${now}  2>> ../log/url_parse_info.stderror.log.${now}
#$PYTHON  url_parse_info2.py $conf_file $kws_file >> ../log/url_parse_info.log.${now} 2>&1
if [ $? -eq 0 ]
then
	echo 'url_parse_info.py is right process'
else
	subject='url_parse_info.py_is_wrong_process'
	content=$subject
	curl -s "http://alarms.ops.qihoo.net:8360/intfs/alarm_intf?group_name=new_recommend&subject=$subject=$content"
	exit
fi


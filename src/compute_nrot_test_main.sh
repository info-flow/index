#!/bin/sh
# test_main的计算nrot的值的 脚本 

now=$(date +%Y%m%d%H%M%S)
echo "本次开始时间:"$now
PYTHON=/da1/db/python27/bin/python

###########################################################################################
#########################       检查是否有完整的文件      #################################
file_pre=../data_test/test_parse_output_main
echo 'file_pre is:'$file_pre

if ls -t ${file_pre}"/2"*".url.tag" | grep -v ".done" | head -n 1 >/dev/null 2>&1
then
	url_tag_pair_file=$(ls -t ${file_pre}"/2"*".url.tag" | grep -v ".done" | head -n 1)
else
	echo 'no pair file, so exit, please check'
	exit
fi
if ls -t ${file_pre}"/2"*".url.tag.done" | grep ".done" | head -n 1 >/dev/null 2>&1
then
	nrot_done_file=$(ls -t ${file_pre}"/2"*".url.tag.done" | grep ".done" | head -n 1)
else
	echo 'no done file, so exit'
	exit
fi
if ls -t ${file_pre}"/2"*".url.info" | grep -v ".done" | head -n 1 >/dev/null 2>&1
then
	url_info_file=$(ls -t ${file_pre}"/2"*".url.info" | grep -v ".done" | head -n 1)
else
	echo 'no info file, so exit, please check'
	exit
fi
if ls -t ${file_pre}"/2"*".url.info.done" | grep ".done" | head -n 1 >/dev/null 2>&1
then
	info_done_file=$(ls -t ${file_pre}"/2"*".url.info.done" | grep ".done" | head -n 1)
else
	echo 'no done file, so exit'
	exit
fi


echo 'pair_file:'$url_tag_pair_file
echo 'done_file:'$nrot_done_file
nrot_match_file=${nrot_done_file%.*}
echo 'match_file:'$nrot_match_file

echo 'info_file:'$url_info_file
echo 'done_file:'$info_done_file
info_match_file=${info_done_file%.*}
echo 'match_file:'$info_match_file
###########################################################################################
######################           获取最新的ctr数据等      #################################
file_pre_save=../data_test/test_nrot_input_main
echo 'file save:'$file_pre_save
nrot_file_save=${file_pre_save}/${now}.url.tag.nrot
info_file_save=${file_pre_save}/${now}.url.info
file_good='../data_prepare/good_data/empyt_file'
file_real_ctr=../data_prepare/real_ctr/test_main.real_ctr.rate.nrot_need.${now}
scp merger82.se.zzzc.qihoo.net:/da1/news/craw_url/ctr/feedback/real_sta/nreal_ctr.rate $file_real_ctr
if [ $? -eq 0 ]
then
	echo 'scp is right'
else
	echo 'scp from 82 to 80 is wrong, so eixt'
	curl -s "http://alarms.ops.qihoo.net:8360/intfs/alarm_intf?group_name=new_recommend&subject=scp_from_82_to_80_failed_this_time,so_exit,main&content=$content"
	exit
fi
today=$(date +%Y%m%d)
if [[ -f  ../data_prepare/good_data/${today} ]]
then
	awk -F '\t' '{print $1;}'  ../data_prepare/good_data/${today} >> $file_real_ctr
fi

###########################################################################################
###########################           计算新的nrot         ################################
file_log=../log/computeReRank.test_main.log.${now}
if [ $nrot_match_file == $url_tag_pair_file ] && [ $info_match_file == $url_info_file ]
then
	echo 'done and ori-file is matched'
	#cat $url_tag_pair_file | $PYTHON computeReRank_ForNewPro.py $nrot_file_save $file_good $file_real_ctr > ../log/computeReRank.test_main.log.${now} 2>&1
	cat $url_tag_pair_file | $PYTHON computeReRank_main.py $nrot_file_save $file_good $file_real_ctr $url_info_file > $file_log 2>&1
	if [ $? -eq 0 ]
	then
		echo 'rerank is right-process'
	else
		echo 'rerank is wrong-process'
		subject='rerank_is_wrong_process,main'
		content=$subject
		curl -s "http://alarms.ops.qihoo.net:8360/intfs/alarm_intf?group_name=new_recommend&subject=$subject&content=$content"
	fi
	## 强制检查一下为0，赋值为100的 ##
	awk -F '\t' '{if($3>0) {print $0} else{print $1"\t"$2"\t100"}}' $nrot_file_save > ${nrot_file_save}.tmp
	num_check_before=$(cat $nrot_file_save | wc -l)
	num_check_after=$(cat ${nrot_file_save}.tmp | wc -l)
	before_after_cha=$(($num_check_before - $num_check_after))
	if [ $before_after_cha -lt 10 ] && [ $before_after_cha -gt 1 ]
	then
		subject='check_has_lessthan_10_not-right,it_is_better_to_check_it,main'
		content=$subject
		curl -s "http://alarms.ops.qihoo.net:8360/intfs/alarm_intf?group_name=new_recommend&subject=$subject&content=$content"
	fi
	if [ $before_after_cha -gt 100 ]
	then
		subject='check_has_big_wrong,not_right_num,'$before_after_cha',must_check_it,exit,main'
		content=$subject
		curl -s "http://alarms.ops.qihoo.net:8360/intfs/alarm_intf?group_name=new_recommend&subject=$subject&content=$content"
		exit
	fi
	#mv ${nrot_file_save}.tmp ${nrot_file_save}
	scp client02.qss.zzbc2.qihoo.net:/da3/nlp/sijianfeng/newscluster/data/quality/quality.current.score .
	$PYTHON computeReWeight.py quality.current.score ${nrot_file_save}.tmp ${nrot_file_save}
	########### 数量检查  ##########
	num_info=$(cat $url_info_file | wc -l)
	num_nrot=$(cat $nrot_file_save | wc -l)
	if [ $num_nrot -gt 20000 ] && [ $num_info -gt 1000 ]
	then
		cp -f $url_info_file $info_file_save
		touch $nrot_file_save.done
		touch $info_file_save.done
	else
		subject='result_num_problem,main'
		content=$subject
		curl -s "http://alarms.ops.qihoo.net:8360/intfs/alarm_intf?group_name=new_recommend&subject=$subject&content=$content"
	fi
	
else
	echo 'done and ori-file is not matched'
fi

now=$(date +%Y%m%d%H%M%S)
echo "本次结束时间:"$now


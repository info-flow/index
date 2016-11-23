#!/usr/bin/python
# -*- coding:utf-8 -*-
# 希望支持多目录数据实时更新 # 拆解为两个文件1)url+'\t'+tag 2)url+'\t'+url_dict_info
# 只需要维持一个正排即可 #

import os
import sys
import time
import commands
reload(sys)
sys.setdefaultencoding('utf-8')
from DE_memory_publicFun import *
try: import simplejson as json
except: import json as json

r = redis.Redis(host='10.172.193.158',port=16797,db=0)
## 正排 ##
zhengpai_map = {}
## 配置文件 ##
conf_file    = sys.argv[1]
## 读取需要检查的文件 目录 ## 输出目录 ##
conf         = loadConf(conf_file)
kws_file     = sys.argv[2]
global_kws   = loadkw1(kws_file) ## 这里可以换 ##
host_name    = loadSiteNameMap('../conf/host.name')
city_name = loadNewLocation('../conf/newlocation')

iter = 0
TimeLength=7*24*3600
TimeBegin=getNowTime('timestamp')

while True:
	time.sleep(1)
#	TimeHere=getNowTime('timestamp')
#	if int(TimeHere)-int(TimeBegin) > TimeLength and getNowTime('notvisual')[] :######
	#if iter > 0: break ## 为了方便试验 ##
	### 每过半个小时更新一下hot标签 ###
#	######     更新是否热点数据    ########## 万一他妹的有人说你的hot不准呢 ##
	conf = loadConf(conf_file)
	if len(conf) == 0: print '请检查文件:'+str(conf_file)+'没有文件，或者没有内容'
	##########################################
	for key, value in conf.items():
		####################################
		##########   找到输入目录  #########
		if key.find('test') != 0: continue # 这里用来保证是试验配置的key读进来 #
		try: file_input_dir = value['input']
		except: print str(key) +'\t'+ 'has no "input"字段, please check:'+str(conf_file); continue
		if str(file_input_dir).find(' ') > 0: print 'error, file has 空格' +str(file_input_dir); continue
		##########   获取输入文件  #########
		str_cmd = 'ls -t '+str(file_input_dir).rstrip().rstrip("/")+'/2* | grep -v ".done$" | head -n 1'
		print 'str_cmd is: '+str(str_cmd)
		try:
			(s, v)  = commands.getstatusoutput(str_cmd)
			if s   == 0:
				#if str(v).find('ls cannot') != 0: file_input = v.split('\n')[0] ## 可能藏有潜在危险 ##
				if not str(v).find('cannot') >= 0: file_input = v.split('\n')[0]
				else: continue
			else: print 'commands.getstatusoutput('+str(str_cmd)+') has wrong, continue'
		except:
			print 'commands.getstatusoutput('+str(str_cmd)+') has wrong, continue'
			continue
		if not os.path.isfile(file_input+'.done'): continue
		#print str(file_input) +'\t has no done file, continue'; continue
		####################################
		##########   找到存储目录  #########
		##########   存储文件建立  #########
		try:
			file_DP_save_dir = value['parse_output']
			if os.path.isdir(file_DP_save_dir): pass
			else:
				os.mkdir(file_DP_save_dir)
			BeginTime    = getNowTime('notvisual')
			file_NR_save = file_DP_save_dir+'/'+str(BeginTime)+'.url.tag'
			file_ZP_save = file_DP_save_dir+'/'+str(BeginTime)+'.url.info'
			if os.path.isfile(file_NR_save): os.remove(file_NR_save)
			if os.path.isfile(file_ZP_save): os.remove(file_ZP_save)
		except:
			print str(key) +'\t'+ 'has no "parse_output"字段, please check:'+str(conf_file)+'\t or os.makedirs错误，权限不够? this error, continue'
			print 'above here value is:\t'+str(value)
			continue
		####################################
		print 'current input file is: \t'+str(file_input)
		####################################
		#######   读取数据到内存中   #######
		timehere = str(getNowTime('visual'))
		NewData = loadNewData(file_input)
		print 'load begin:\t'+timehere+'\tload end:\t'+str(getNowTime('visual'))+'\t file name is:\t'+str(file_input)
		#######   判断是否在正排中   #######
		for url, url_value in NewData.items():
			if url in zhengpai_map: continue
			else:
				try: url_info_map = r.hgetall(url+'@uinfo')
				except: print url +'\t error:cannot get from redis'
				if len(url_info_map) < 1: print url +'\terror: get less data from redis'
				url_info = getInfoAll2(url, {}, host_name, url_info_map)
				url_info['all_feature'] = getFeature2(url, url_info_map)
				url_info['work_feature'] = []
				keys_list = getNewTags2(url, global_kws, url_info_map)
				city = get_city2(url, url_info_map)
				if len(city) > 0 : keys_list.append('kloc_'+city)
				tag_cluster_list = getClusterTags2(url, url_info_map)
				if len(tag_cluster_list)>0: keys_list = keys_list +tag_cluster_list
				#keys_list = [i.replace('"','') for i in keys_list]
				if len(keys_list)==0: print url+'\terror: get no ntag this url' +str(url); continue
				urltag = ";".join(keys_list)
				appendix = {"ntag":urltag, "source":"multi,"+str(key), 'must_add':'0'}
				for k, v in appendix.items(): url_info[k] = v
				######       增加正排数据       ##########
				zhengpai_map[url] = url_info
		## 
		## (3) 保存正排结果 同时保存url+ntag ##
		if len(zhengpai_map)>1000:
			print 'zhengpai num > 1000, begin save'
			if os.path.isfile(file_ZP_save): os.remove(file_ZP_save)
			if os.path.isfile(file_NR_save): os.remove(file_NR_save)
			print 'result saved into:'+str(file_ZP_save)
			print 'result saved into:'+str(file_NR_save)

			z_handle = open(file_ZP_save, 'aw')
			n_handle = open(file_NR_save, 'aw')
			##########      后缀检查     ########
			tmp = key.split('_')
			if len(tmp) == 3: fixFlag = '_' + str(tmp[2])
			else: fixFlag = ''
			##########      保存       ###########
			for url, url_value in NewData.items():
			#	print 'there'
				if url in zhengpai_map:
					info     = zhengpai_map[url]
				else: continue
				if 'ntag' not in info: continue
				for tag in info['ntag'].split(';'):
			#		print 'most in'
					str_save = url +'\t'+ str(tag)+str(fixFlag) +'\n'
					n_handle.write(str_save)
				str_save = url+'\t'+json.dumps(info, ensure_ascii=False)+'\n'
				z_handle.write(str_save)
			z_handle.close()
			n_handle.close()
			str_cmd = 'touch '+str(file_ZP_save)+'.done'
			(s, v)  = commands.getstatusoutput(str_cmd)
			print 'str_cmd is:'+str(str_cmd)+', s: '+str(s)+', v: '+str(v)
			str_cmd = 'touch '+str(file_NR_save)+'.done'
			(s, v)  = commands.getstatusoutput(str_cmd)
			print 'str_cmd is:'+str(str_cmd)+', s: '+str(s)+', v: '+str(v)

	#####################################################################
		print 'save end here :'+str(getNowTime('visual'))
		## (4) 删除已经处理的数据  ##
		if str(file_input).find('20') > 0:
			str_cmd = 'rm -f ' +str(file_input)
			print 'str_cmd is:'+str(str_cmd)
			(s, v)  = commands.getstatusoutput(str_cmd)
			str_cmd = 'rm -f ' +str(file_input)+ '.done'
			print 'str_cmd is:'+str(str_cmd)
			(s, v)  = commands.getstatusoutput(str_cmd)
		else:
			print str(file_input) +' maybe a wrong delete-file you want, please check'
	######################################################################
	####        需要加个时间限制，7天外数据清理                     ######
	
	#iter += 1
	#if iter > 0: break
	## 更新完之后删除done及对应文件 ## 避免重复计算 ##
	


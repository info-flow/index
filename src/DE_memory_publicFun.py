#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import re
import sys
import redis
import time
import hashlib
import urllib
import urllib2
import commands
try: import simplejson as json
except: import json as json
print 'json version:'+str(json.__version__)+';\t json name:'+str(json.__name__)
print 'waring: if json is not simplejson, it will take a longer time to load initial data'

r = redis.Redis(host='10.172.193.158',port=16797,db=0)
l = redis.Redis(host='10.172.193.158',port=16797,db=2)
def getValueFromRedis(url, key):
	res = ""
	table_url = url+"@uinfo"
	if r.hexists(table_url, key):
		res = r.hget(table_url, key)
	return res

## load 函数 ##
def loadConf(filepath):
	res = {}
	if os.path.isfile(filepath):
		for line in open(filepath):
			line  = line.strip()
			if len(line) == 0: continue
			if line[0] == "#": continue
			words = line.split('\t')
			if len(words) < 2: continue
			key   = words[0].strip()
			try:
				value = json.loads(words[1].strip())
			except: continue
			if key in res: continue
			res[key] = value
		#print 'load conf from : '+str(filepath)
	else: print 'not find file named:'+str(filepath)
	return res
def loadDP(filepath):
	res = {}
	if os.path.isfile(filepath):
		for line in open(filepath):
			line  = line.strip()
			if len(line) == 0: continue
			words = line.split('\t')
			if len(words) < 2: continue
			tag   = words[0].strip()
			try:
				#info  = json.loads(words[1])
				info = words[1].strip()
			except: continue
			if tag in res: continue
			res[tag] = info
	else: print 'no file named:'+str(filepath)
	print 'load data :'+str(len(res))+'\t, from:'+str(filepath)
	return res
def loadZP(filepath):
	res = {}
	if os.path.isfile(filepath):
		for line in open(filepath):
			line  = line.strip()
			if len(line) == 0: continue
			words = line.split('\t')
			if len(words) < 2: continue
			tag   = words[0].strip()
			try:
				info  = json.loads(words[1])
			except: continue
			if tag in res: continue
			res[tag] = info
	else: print 'no file named:'+str(filepath)
	print 'load data :'+str(len(res))+'\t, from:'+str(filepath)
	return res
def loadNrot(dirpath):
	res = {}
	# 先找到个最新的文件 #
	print 'nort dir is :\t'+str(dirpath)
	str_cmd = 'ls -t '+str(dirpath)+ '/*nrot* |grep ".done"| head -n 1'
	print 'cmd string is:\t'+str(str_cmd)
	v = ''
	while v == '':
		try:
			(s, v)  = commands.getstatusoutput(str_cmd)
			print   s
			if s    == 0:
				print 'v length is:'+str(len(v.split('\n')))
				filepath = str(v.split('\n')[0])
				print 'nrot done file:\t'+str(filepath)
				#for key in v:
				filepath = filepath.replace('.done', '')
				print 'nrot file is:\t'+str(filepath)
				break
			else:
				filepath = str(v.split('\n')[0])
				filepath = filepath.repalce('.done', '')
				if os.path.isfile(filepath): break
		except:
			print 'not find the last-new of nrot file this time'
	if os.path.isfile(filepath):
		for line in open(filepath):
			line  = line.strip()
			if len(line) == 0: continue
			words = line.split('\t')
			if len(words) < 3: continue
			url   = words[0].strip()
			tag   = words[1].strip()
			nrot  = words[2].strip()
			key   = url+'\t'+tag
			if key in res: continue
			res[key] = nrot
	else: print 'no file named:'+str(filepath)
	print 'load data :'+str(len(res))+'\t, from:'+str(filepath)
	return res
def loadNewData(filepath):
	res = {}
	if os.path.isfile(filepath):
		for line in open(filepath):
			line  = line.strip()
			if len(line) == 0: continue
			words = line.split('\t')
			if len(words) < 1: continue
			url   = words[0].strip()
			if url in res: continue
			res[url] = 1
	else: print 'no file named:'+str(filepath)
	print 'load data :'+str(len(res))+'\t, from:'+str(filepath)
	return res
	
def loadNewDataYDZX(filepath):
	res = {}
	res_ntag = {}
	if os.path.isfile(filepath):
		for line in open(filepath):
			line  = line.strip()
			if len(line) == 0: continue
			words = line.split('\t')
			if len(words) < 2: continue
			tag   = words[0].strip()
			url   = words[1].strip()
			if url in res: pass
			else: res[url] = 1
			if url in res_ntag:
				if tag in res_ntag[url]: continue
				else: res_ntag[url].append(tag)
			else:
				res_ntag[url] = [tag]
	else: print 'no file named:'+str(filepath)
	print 'load data :'+str(len(res))+'\t, from:'+str(filepath)
	return res, res_ntag
def loadNewDataChain(file_tag_urls, file_url_ntag):
	tag_urls_map = {}
	url_ntag_map = {}
	all_urls     = {}
	if os.path.isfile(file_tag_urls):
		for line in open(file_tag_urls):
			line  = line.strip()
			if len(line) <= 0: continue
			words = line.split('\t')
			if len(words) < 2: continue
			tag   = words[0].strip()
			urls  = words[1].strip()
			if tag in tag_urls_map: continue
			tag_urls_map[tag] = urls
			for url in urls.split('\1'):
				if url in all_urls: continue
				all_urls[url] = 1
	if os.path.isfile(file_url_ntag):
		for line in open(file_url_ntag):
			line = line.strip()
			if len(line) <= 0: continue
			words = line.split('\t')
			if len(words) < 2: continue
			url   = words[0].strip()
			ntag  = words[1].strip()
			ntag  = ntag.replace(',', ';')
			if url in url_ntag_map: continue
			url_ntag_map[url] = ntag
			if url in all_urls: continue
			all_urls[url] = 1
	return all_urls, tag_urls_map, url_ntag_map

def getNowTime(time_type='visual'):
	now = ''
	if time_type == 'visual':
		now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
	elif time_type == 'timestamp':
		now = str(int(time.time()))
	else:
		now = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time()))
	return now

def getNewTags2(url, global_kws, url_info_map):
	# 获取新的tag #
	res = []
	try:
		tbtag = url_info_map['tbtag']
	except: tbtag = ''
	if len(tbtag)>0:
		if "t"+str(tbtag) in res:pass
		else: res.append("t"+str(tbtag))
	
	try:
		kws = url_info_map['kws']
	except: kws = ''
	breakflag = 0
	if len(kws)>0 and len(tbtag)>0:
		words = kws.split('|')
		for kw in words:
			if kw.find(' ') != -1: kw = kw.split(' ')[0]; breakflag=1
			kw = kw.strip()
			if len(kw)>0:
				rkw = 'k'+str(tbtag)+'_'+str(kw)
				if rkw not in res and rkw in global_kws: res.append(rkw)
			if breakflag==1: break
	try:
		btag = url_info_map['btag']
	except: btag = ''
	subtag = ""
	if len(btag)>0 and len(tbtag)>0:
		subtag=btag.split(';', -1)[0].split(',')[0]
		if subtag.find('|') != -1:
			kw = subtag.split('|')[1]
			skw = 's'+str(tbtag)+"_"+str(kw)
			if skw in res: pass
			else: res.append(skw)
		else:
			if len(subtag)>0:
				skw = 's'+str(tbtag)+'_'+str(subtag)
				if skw in res: pass
				else: res.append(skw)
	return res

def getNewTagsAll2(url, global_kws, url_info_map):
	#print 'len of global_kws is:\t'+str(len(global_kws))
	#print 'len of url_info_map is:\t'+str(len(url_info_map))

	res = []
	try:
		tbtag = url_info_map['tbtag']
	except: tbtag = ''
	if len(tbtag)>0:
		if "t"+str(tbtag) in res:pass
		else: res.append("t"+str(tbtag))
	
	try:
		kws = url_info_map['kws']
	except: kws = ''
	breakflag = 0
	if len(kws)>0 and len(tbtag)>0:
		words = kws.split('|')
		for kw in words:
			if kw.find(' ') != -1: kw = kw.split(' ')[0]; breakflag=1
			kw = kw.strip()
			if len(kw)>0:
				rkw = 'k'+str(tbtag)+'_'+str(kw)
				if rkw not in res and rkw in global_kws: res.append(rkw)
			if breakflag==1: break
	try:
		btag = url_info_map['btag']
	except: btag = ''
	subtag = ""
	if len(btag)>0 and len(tbtag)>0:
		subtag=btag.split(';', -1)[0].split(',')[0]
		if subtag.find('|') != -1:
			kw = subtag.split('|')[1]
			skw = 's'+str(tbtag)+"_"+str(kw)
			if skw in res: pass
			else: res.append(skw)
		else:
			if len(subtag)>0:
				skw = 's'+str(tbtag)+'_'+str(subtag)
				if skw in res: pass
				else: res.append(skw)

		## 检查可能项 ##
		sub = btag.split(';')
		for part in sub:
			if part.find('kws_title') != -1:
				if part.find(':') != -1 and part.find('|') != -1:
					kws = part.split(':')[1].split('|')
					for kw in kws:
						rkw = 'k'+str(tbtag)+'_'+str(kw)
						if rkw not in res and rkw in global_kws: res.append(rkw)
			if part.find('kws_all')   != -1:
				if part.find(':') != -1 and part.find('|') != -1:
					kws = part.split(':')[1].split('|')
					for kw in kws:
						rkw = 'k'+str(tbtag)+'_'+str(kw)
						if rkw not in res and rkw in global_kws: res.append(rkw)
				
			if part.find('kws_all_entity') != -1:
				if part.find(':') != -1 and part.find('|') != -1:
					kws = part.split(':')[1].split('|')
					#print str(url)+'\t (in getTagsAll)kws_all_entity is:'+str(kws)
					for kw in kws:
						if kw.find(',') != -1: continue
						rkw = 'k'+str(tbtag)+'_'+str(kw)
						#if rkw not in res and rkw in global_kws: res.append(rkw)
						if rkw not in res: res.append(rkw)
			if part.find('kws_title_entity') != -1:
				if part.find(':') != -1 and part.find('|') != -1:
					kws = part.split(':')[1].split('|')
					#print str(url)+'\t (in getTagsAll) kws_title_entity is:'+str(kws)
					for kw in kws:
						if kw.find(',') != -1: continue
						rkw = 'k'+str(tbtag)+'_'+str(kw)
						#if rkw not in res and rkw in global_kws: res.append(rkw)
						if rkw not in res: res.append(rkw)
	return res
## 读取 函数 ##
## 保存 函数 ##
def loadkw1(filekw):
	# 新的tag只有关键词的 #
	global_kws = {}
	if os.path.isfile(filekw):
		for line in open(filekw):
			line = line.strip()
			if len(line) == 0: continue
			words = line.split('\t')
			if len(words) < 2: continue
			tag=words[0].strip()
			kw =words[1].strip()
			rkw = 'k'+str(tag)+'_'+str(kw)
			if rkw in global_kws: continue
			global_kws[rkw] = 1
		print "加载承天关键词，个数为："+str(len(global_kws))
		print "type is :"+str(type(global_kws))
	else: print "warning and error: no kw_word from chengtian, it will cost a much time for building daopai"
	return global_kws
def loadSiteNameMap(filemap):
	host_name = {}
	if os.path.isfile(filemap):
		print "load host.name.map here ..."
		for line in open(filemap):
			line = line.strip()
			if len(line) == 0: continue
			words = line.split('\t')
			if len(words)<=1: continue
			host = words[0].strip()
			name = words[1].strip()
			if host in host_name: continue
			host_name[host] = name
		print "load host.name.map.paris is:"+str(len(host_name))
		for k, v in host_name.items():
			print str(k)+"\t"+str(v)
			break
	else: print "error: no host.name.map file named: "+str(filemap)
	return host_name
def getClusterTags2(url, url_info_map):
	res = []
	try:
		cluster_label = url_info_map['cluster_label']
	except: cluster_label = ''
	try:
		tbtag = url_info_map['tbtag']
	except: tbtag = ''
	if len(cluster_label)>0 and len(tbtag)>0:
		clusters_list = cluster_label.split(' ')
		res = ['c'+str(tbtag)+'_'+str(i) for i in clusters_list]
	return res
def getTrigger2(url, url_info_map):
	res = ""
	if isinstance(url, unicode):
		url = url.encode('utf-8')
	elif not isinstance(url, str):
		url = str(url)
	#print 'getTrigger function, url is:'+str(url)
	m = hashlib.md5(url).hexdigest()
	try:
		tbtag = url_info_map['tbtag']
	except: tbtag = ''
	if len(m)>0 and len(tbtag)>0:
		res = tbtag+'_'+m[8:24]
	return res
def getInfoAll2(url, add, host_name, url_info_map):
	result_save = {}
	#try:
	#	url_info_map = r.hgetall(url+'@uinfo')
	#except:
	#	result_save = {}
	#	return result_save
	try:
		cluster_label = url_info_map['cluster_label']
	except: cluster_label = ''
	if len(cluster_label)>0:
		result_save.setdefault('cluster_label', cluster_label)
	result_save.setdefault("url", getOldTranscode(url))
	result_save.setdefault('ori_url', url)
	result_save.setdefault("recommInfo", "recom")
	try:
		click = url_info_map['click']
	except: click = ''
	if len(click)==0: click = 0
	result_save.setdefault("click", str(click))
	try:
		pv = url_info_map['pv']
	except: pv = ''
	if len(pv)==0: pv = 0
	result_save.setdefault("show", str(pv))
	result_save.setdefault('pv', str(pv))
	try:
		ctr = url_info_map['ctr']
	except: ctr = ''
	if len(ctr) == 0: ctr = '0'
	result_save.setdefault("ctr", str(ctr))
	if len(ctr) == 0: rot = '0'
	else: rot = int(float(ctr)*10000)
	result_save.setdefault("rot", str(rot))
	result_save.setdefault("nrot", str(rot))
	result_save.setdefault("source", "")
	if add.has_key("interest_rank"):
		score = add['interest_rank']
		rank = score
	else:
		score = ""
		rank = ""
	result_save.setdefault("score", str(score))
	result_save.setdefault("rank", str(rank))
	result_save.setdefault("idx", '0')
	try:
		w = url_info_map['w']
	except: w = ''
	if len(w)<=0: w = '0'
	result_save.setdefault("w", str(w))
	try:
		pdate_raw = url_info_map['pdate']
	except: pdate_raw = ''
	if len(pdate_raw)>0:
		pdate_raw = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(pdate_raw)))
	else: pdate_raw = ""
	result_save.setdefault("pdate_raw", str(pdate_raw))
	try:
		tbtag = url_info_map['tbtag']
	except: tbtag = ''
	result_save.setdefault("tag", tbtag)
	result_save.setdefault("tbtag", tbtag)
	try:
		pat = url_info_map['pat']
	except: pat = ''
	result_save.setdefault('pat', pat)
	try:
		type = url_info_map['type']
	except: type = ''
	result_save.setdefault("type", type)
	try:
		pdate = url_info_map['live_pdate']
	except: pdate = ''
	if len(pdate)<=0:
		try: pdate = url_info_map['pdate']
		except: pdate = ''
	result_save.setdefault("pdate", pdate)
	try: tmp = url_info_map['pdate']
	except: tmp = ''
	result_save.setdefault("timestamp", tmp)
	now = int(time.time())
	if len(pdate)<=0: life=''
	else: life = abs((now-int(pdate)+3600)/3600)+1
	result_save.setdefault("life", str(life))
	try:
		md5 = url_info_map['md5']
	except: md5 = ''
	result_save.setdefault("docid", md5)
	try:
		title = url_info_map['title']
	except: title = ''
	result_save.setdefault("title", '')
	try:
		kws = url_info_map['kws']
	except: kws = ''
	if len(kws)>0 and kws.find(' ') != -1:
		kws = kws.split(' ')[0]
	result_save.setdefault("kws", str(kws))
	try:
		image = url_info_map['image']
	except: image = ''
	result_save.setdefault("image", image)
	result_save.setdefault("image_url", image)
	try:
		site = url_info_map['site']
	except: site = ''
	result_save.setdefault("domain", site)
	result_save.setdefault("site", site)
	try:
		topic = url_info_map['topic']
	except: topic = ''
	result_save.setdefault("topic", topic)
	try:
		local = url_info_map['local']
	except: local = ''
	result_save.setdefault("local", local)
	try:
		comment_score = url_info_map['comment_score']
	except: comment_score = ''
	if len(comment_score)==0: comment_score = '0'
	result_save.setdefault("comment", comment_score)
	try:
		kws_title_entity = url_info_map['kws_title_entity']
	except: kws_title_entity = ''
	result_save.setdefault("kwste", kws_title_entity)
	stag = get_stag2(url, url_info_map)
	result_save.setdefault('stag', str(stag))
	sss = get_scd_special_score2(url, url_info_map)
	if len(sss)==0: sss=0
	result_save.setdefault("scd_special_score", str(sss))
	src = "360资讯"
	try:
		fro = url_info_map['from']
	except: fro = ''
	try:
		site = url_info_map['site']
	except: site = ''
	if len(fro)>0: src = fro
	else:
		if len(site)>0 and site in host_name: src = host_name[site]
	result_save.setdefault("src", str(src))
	try:
		rptid = url_info_map['rptid']
	except: rptid = ''
	dupid=""
	if l.hexists(url+"@dupId", url): dupid=l.hget(url+"@dupId", url)
	if len(dupid)>0: result_save.setdefault("dupid", str(dupid))
	else: result_save.setdefault("dupid", str(rptid))
	result_save.setdefault("rptid", str(rptid))
	kws = kws
	if len(stag)>0:
		if stag.find('|') != -1: stag_pre = stag.split('|')[0]
		else: stag_pre = stag
		result_save.setdefault('rec', str(stag_pre)+";"+str(kws))
	else:
		result_save.setdefault("rec", str(kws))
	##(2) 补充 add信息 ##
	for k, v in add.items():
		result_save[k] = v
	##
	##result_save.setdefault('recdatatype', result_save['source'])
	return result_save
	

def get_city2(url, url_info_map):
	city = ''
	try:
		btag = url_info_map['btag']
	except:
		return city
	if len(btag)>0:
		#print btag
		words = btag.split(';')
		sss = words[len(words)-2]
		#print "chengshi:"+str(sss)
		if sss.find(',') != -1:
			city = sss.split(',')[0]
			if city.find('city:') != -1:
				city = city.replace('city:', '')
			else: city = ""
	return city
def getOldTranscode(url):
	#if url.find("m.haosou.com") != -1 or url.find("m.news.haosou") != -1 or url.find("m.news.so") != -1 or url.find("m.so.com") != -1 or url.find("weixin.qq.com") != -1 or url.find(    "dic28.se.zzbc.qihoo.net") != -1:
	#	return url
	sha1obj = hashlib.sha1()
	sha1obj.update(url+"#:DHiE'Q.@X]G:Pz'vZa`@e`7YNa<fEjRY(sL|94?\"SJXi85'.h5ZY9X7#hj5p/k")
	m = sha1obj.hexdigest()
	newurl = "http://m.so.com/index.php?a=newTranscode&u="
	newurl += re.sub("/","%2F",urllib2.quote(url))
	newurl += "&m="
	newurl += m
	newurl += "&tc_mode=news_recom&src=360aphone_news"
	return newurl
def get_scd_special_score2(url, url_info_map):
	res=''
	try:
		btag=url_info_map['btag']
	except:
		return res
	#print "current btag is : "+str(btag)
	if len(btag)>0:
		words = btag.split(';')
		#print "current split by ; is :"+str(words)
		sss = str(words[len(words)-2])
		#print "current word is : "+str(sss)
		if sss.find('special_score') != -1 and sss.find(',') != -1:
			keys = sss.split(',')
			res = keys[1]
			#print "current get special_score : "+str(res)
	return res
def get_stag2(url, url_info_map):
	res = ''
	try:
		btag = url_info_map['btag']
	except:
		return res
	if len(btag)>0:
		words = btag.split(';')
		#print "current split by ; is :"+str(words)
		sss = str(words[len(words)-2])
		#print "current word is : "+str(sss)
		if sss.find('special_score') != -1 and sss.find(',') != -1:
			keys = sss.split(',')
			res = keys[1]
	return res
def loadNewLocation(file_loc):
	location = {}
	if os.path.isfile(file_loc):
		print "load location-name here ... "
		file = open(file_loc)
		line = file.readline().strip()
		num = 0
		while line != "":
			loc = line.strip()
			if loc not in location:
				#print line
				location[loc] = 1
			num += 1
			if num%10000 == 0: print "current location in pos : "+str(num)
			line = file.readline().strip()
		file.close()
		print "load location name : "+str(len(location))+" 个"
		#print "the first location is :"+str(location[0])
	else: print "error: no locationSingle file "+str(file_loc)
	return location
def mapInfoToOpstate(btag, pat):
	pat_elem_list = pat.split(',')
	if 'sexac' in pat_elem_list: return '7' 
	elif 'gaoxiao' in pat_elem_list: return '12'
	elif 'qgc' in pat_elem_list: return '13'
	first_class  = btag.split(',')[0]
	first_main_calss = first_class.split('|')[0]
	if first_class == '社会|情感': return '9' 
	if first_main_calss == '故事': return '10'
	return '0' 

def getFeature2(url, url_dict_info):#, host_name, hot_data, good_data, global_kws): 不再提供hot数据的判断 ##
	## url 是url ##
	## url_dict_info 是url的字典信息 ## 从redis读取的 ##
	res = {}
	## 通用的 feature ##
	all = url_dict_info
	if 'src' in all: res['v_src']=[all['src']]
	if 'site' in all:res['v_site']=[all['site']]
	#if 'sex_score' in all: res['f_sex_score']=all['sex_score']
	if 'pat' in all: res['v_pat']=all['pat'].split(',')
	## 特殊的feature ##
	city = get_city2(url, all)
	if len(city)>0: res['f_city'] = city
	try: opstate = all['opstate']
	except: opstate = ''
	try: btag = all['btag']
	except: btag = ''
	try: pat  = all['pat']
	except: pat = ''
	algor_state = mapInfoToOpstate(btag, pat)
	set_weak = set(['6', '7', '8', '9', '10', '11', '13', '14'])
	if opstate in set_weak: res['f_weak'] = '1'
	else:
		set_sop = set(['6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18'])
		if algor_state in set_weak and not opstate in set_sop:
			res['f_weak'] = '1'
		else: res['f_weak'] = '0'
	return res

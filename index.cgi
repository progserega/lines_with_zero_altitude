#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cgi
import sys
import os
import psycopg2
import psycopg2.extras
import db_config as config
import math
import tempfile

def get_id_of_all_ways_in_relations_for_vl_by_node_id(lines,node_id):
	try:
		# Получаем список отношений, в которых состоят линии, которым принадлежит искомая точка:
		if config.debug==True:
			print("""select relation_id,max(version) as version from relation_members where member_type='Way' and member_id in (select way_id from way_nodes where cast(way_id as text) || '-' || cast(version as text) in (select cast(way_id as text) || '-' || cast(max(version) as text) from way_nodes where node_id=%(node_id_to_find)d group by way_id) group by way_id) and cast(relation_id as text) || '-' || cast(version as text) in (select cast(relation_id as text) || '-' || cast(version as text) from relation_tags where  cast(relation_id as text) || '-' || cast(version as text) in (select cast(relation_id as text) || '-' || cast(max(version) as text) from relation_tags group by relation_id) and k='power' and v='line') group by relation_id""" % {"node_id_to_find":node_id_to_find})
		cur.execute("""select relation_id,max(version) as version from relation_members where member_type='Way' and member_id in (select way_id from way_nodes where cast(way_id as text) || '-' || cast(version as text) in (select cast(way_id as text) || '-' || cast(max(version) as text) from way_nodes where node_id=%(node_id_to_find)d group by way_id) group by way_id) and cast(relation_id as text) || '-' || cast(version as text) in (select cast(relation_id as text) || '-' || cast(version as text) from relation_tags where  cast(relation_id as text) || '-' || cast(version as text) in (select cast(relation_id as text) || '-' || cast(max(version) as text) from relation_tags group by relation_id) and k='power' and v='line') group by relation_id""" % {"node_id_to_find":node_id_to_find})
		relation_ids = cur.fetchall()
	except:
		print ("I am unable fetch data from db");sys.exit(1)
	# Получаем имена отношений:
	for relation in relation_ids:
		line={}
		line["relation_id"]=relation[0]
		line["relation_version"]=relation[1]
		try:
			cur.execute("""select v from relation_tags where  relation_id=%(relation_id)d and version=%(version)d and k='name'""" % {"relation_id":line["relation_id"], "version":line["relation_version"]} )
			name = cur.fetchall()
		except:
			print ("I am unable fetch data from db");sys.exit(1)
		if len(name) == 0:
			if config.debug==True:
				print("Отношение линии %d не содержит тега 'name'! Пропуск!" % line["relation_id"])
			continue
		line["line_name"]=name[0][0]
		# Берём список линий из отношения:
		try:
			cur.execute("""select member_id from relation_members where  relation_id=%(relation_id)d and version=%(version)d and member_type='Way' order by sequence_id""" % {"relation_id":line["relation_id"], "version":line["relation_version"]} )
			ways = cur.fetchall()
		except:
			print ("I am unable fetch data from db");sys.exit(1)
		line["ways"]=[]
		for way in ways:
			if config.debug==True:
				print("way =",way)
				print("way[0] =",way[0])
			line["ways"].append(way[0])
		lines[line["line_name"]]=line
		if config.debug==True:
			print("line['ways'] =",line["ways"])
	return lines

def get_id_of_all_ways_for_vl_by_node_id(lines,node_id):
	try:
		# Берём список идентификаторов линий и наименований этих линий, которым принадлежит эта точка:
		if config.debug==True:
			print("""select way_id,v from way_tags where cast(way_id as text) || '-' || cast(version as text) in ( select cast(way_id as text) || '-' || cast(max(version) as text) as tt from way_nodes where node_id=%(node_id_to_find)d group by way_id) and way_id in (select way_id from way_tags where (k='power' and (v='line' or v='cable'))) and k='name'""" % {"node_id_to_find":node_id})
		cur.execute("""select way_id,v from way_tags where cast(way_id as text) || '-' || cast(version as text) in ( select cast(way_id as text) || '-' || cast(max(version) as text) as tt from way_nodes where node_id=%(node_id_to_find)d group by way_id) and way_id in (select way_id from way_tags where (k='power' and (v='line' or v='cable'))) and k='name'""" % {"node_id_to_find":node_id})
		# Загоняем значения в set(), преобразуя из списка, т.к. в set() будут только уникальные значения:
		ways_use_finded_node = cur.fetchall()
	except:
		print ("get_id_of_all_ways_for_vl_by_node_id(): Error fetch data from db");sys.exit(1)
	if config.debug:
		print("get_id_of_all_ways_for_vl_by_node_id: ways_use_finded_node =", ways_use_finded_node)
	# Теперь нам нужно найти все линии, с таким именем:
	for way in ways_use_finded_node:
		line={}
		line["line_name"]=way[1]
		line["ways"]=set()
		line["ways"].update(get_ways_by_name(line["line_name"]))
		if config.debug:
			print("get_id_of_all_ways_for_vl_by_node_id: get_ways_by_name(%s):" %line["line_name"],get_ways_by_name(line["line_name"]))
		# Добавляем данные о линии только если такой линии там нет (она могла быть добавлена как отношение):
		if not line["line_name"] in lines:
			lines[line["line_name"]]=line
	return lines

def get_ways_by_name(vl_name):
	try:
		cur.execute("""select way_id from way_tags where cast(way_id as text) || '-' || cast(version as text) in ( select cast(way_id as text) || '-' || cast(max(version) as text) as tt from way_tags group by way_id) and way_id in (select way_id from way_tags where k='power' and (v='line' or v='cable')) and k='name' and v='%(vl_name)s'""" % {"vl_name":vl_name})
		rows = cur.fetchall()
	except:
		print ("get_ways_by_name(): I am unable fetch data from db");sys.exit(1)
	if config.debug:
		print("get_ways_by_name: rows=",rows)
	# Двумерный массив в одномерный список:
	way_id_list=[]
	for line in rows:
		way_id_list.append(line[0])
	return way_id_list
	
def print_html_node_report(lines):
	print("""
		<TABLE BORDER>
		<TR>    
				<TH COLSPAN=3>Линии содержащие опоры с отсутствующей высотой, или высотой основания, равной 0 метров над уровнем моря.</TH>
		</TR>
		""")
	for line_name in lines:
		line=lines[line_name]
		if len(line["ways"]) == 0 or len(line["node_lists"]) == 0:
			continue
		print("""
		<TR>
				<TH COLSPAN=3>Наименование линии "%(line_name)s"</TH>
		</TR>
		<TR>
		<TH>Номер опоры</TH>
		<TH>Высота над уровнем моря основания опоры</TH>
		<TH>Ссылка на карту</TH>
		</TR>


		""" % {"line_name":line_name})
		
		for way_id in line["ways"]:
			# Пропускаем линии, в которых нет опор 
			if not way_id in line["node_lists"]:
				continue
			print("""<TR>    
					<TH COLSPAN=3>Участок линии с id=%(way_id)d</TH>
			</TR>""" % {"way_id":way_id,})
			node_lists=line["node_lists"][way_id]
			for node in node_lists:
				if "ref" in node["tags"]:
					node_ref=node["tags"]["ref"]
				else:
					node_ref="опора без имени"
				if "ele" in node["tags"]:
					try:
						ele=float(node["tags"]["ele"])
					except Exception:
						ele=float(0)
				else:
					ele=float(0)

				print("""<TR>
				 <TD>%(node_ref)s</TD>
				 <TD>%(ele).2f</TD>
				 <TD>%(url)s</TD>
				 </TR>""" % \
				 {"node_ref":node_ref, \
				 "ele":ele, \
				 "url":"""<a target="_self" href="http://map.prim.drsk.ru/#map=18/%(lat)f/%(lon)f&layer=MFxzlkj&poi=La2">карта</a>""" % {"lat":node["lat"],"lon":node["lon"]} } )

	print("</TABLE>")
	
def print_html_line_report(lines):
	index=1
	print("""
		<TABLE BORDER>
		<TR>    
				<TH COLSPAN=4>Наименования линий, содержащих опоры с отсутствующей высотой, или высотой основания, равной 0 метров над уровнем моря.</TH>
		</TR>
		<TR>
		<TH>№</TH>
		<TH>Имя линии</TH>
		<TH>Количество опор с нулевой высотой</TH>
		<TH>Ссылка на карту</TH>
		</TR>
		""" )
	for line_name in lines:
		line=lines[line_name]
		if len(line["ways"]) == 0 or len(line["node_lists"]) == 0:
			continue

		num_nodes=0
		lat=0
		lon=0
		for way_id in line["ways"]:
			# Пропускаем линии, в которых нет опор 
			if not way_id in line["node_lists"]:
				continue
			node_lists=line["node_lists"][way_id]
			# для ссылки берём первую попавшуюся опору с нулевой высотой:
			if lat==0 and lon==0:
				lat=node_lists[0]["lat"]
				lon=node_lists[0]["lon"]
			num_nodes+=len(node_lists)
		print("""<TR>
				 <TD>%(index)d</TD>
				 <TD>%(line_name)s</TD>
				 <TD>%(zero_ele_num)d</TD>
				 <TD>%(url)s</TD>
				 </TR>""" % \
				 {"index":index, \
				 "line_name":line_name, \
				 "zero_ele_num":num_nodes, \
				 "url":"""<a target="_self" href="http://map.prim.drsk.ru/#map=18/%(lat)f/%(lon)f&layer=MFxzlkj&poi=La2">карта</a>""" % {"lat":lat,"lon":lon} } )
		index+=1

	print("</TABLE>")

def print_text_line(lines):
	for line_name in lines:
		line=lines[line_name]
		if len(line["ways"]) == 0 or len(line["node_lists"]) == 0:
			continue
		#print("line['ways'] =" , line["ways"])
		#print("line['node_lists'] =" , line["node_lists"])
		print("======= %s =========" % line_name)
		for way_id in line["ways"]:
			# Пропускаем линии, в которых нет пролётов (например из двух точек, одна из которых не имеет 'ref')
#if not way_id in line["prolet_lists"]:
#				continue
			print("======= (way_id=%d)=========" % way_id)
			if way_id in line["node_lists"]:
				node_lists=line["node_lists"][way_id]
				for node in node_lists:
					print_node(node)
			else:
				print ("%d not have node_lists" % way_id)



def print_node(node):
	print ("========= print_node() ========================")
	print("node_id = %d" % node["node_id"])
	print("lat = %f" % node["lat"])
	print("lon = %f" % node["lon"])
	print("ele = %f" % node["ele"])
	print("tags:")
	for k in node["tags"]:
		print("%s = %s" % (k,node["tags"][k]))

def generate_node_list(line):
	line_name=line["line_name"]
	line["node_lists"]={}
	for way_id in line["ways"]:
		node_list=[]
		if config.debug:
			print("line['ways'] =",line["ways"])
			print("way_id =",way_id)
		#print(line_name)
		# Берём список идентификаторов точек в переданной линии, отсортированных по последовательности:
		if config.debug:
			print("""select node_id from way_nodes where way_id=%(way_id)d and (cast(node_id as text) || '-' || cast(version as text) ) in (select (cast(node_id as text) || '-' || cast(max(version) as text) ) as tt from way_nodes where way_id=%(way_id)d group by node_id) order by sequence_id;""" % { "way_id":way_id })
		cur.execute("""select node_id from way_nodes where way_id=%(way_id)d and (cast(way_id as text) || '-' || cast(version as text) ) in (select (cast(way_id as text) || '-' || cast(max(version) as text) ) as tt from way_nodes where way_id=%(way_id)d group by way_id) order by sequence_id;""" % { "way_id":way_id })
		rows = cur.fetchall()
		for row in rows:
			node={}
			node["node_id"]=row[0]
			# Заполняем данные по опорам:
			#cur.execute("""select latitude,longitude from nodes where node_id=%(node_id)d""" % {"node_id":row[0]})
			if config.debug:
				print("""select node_id,max(version),latitude,longitude from nodes where node_id=%(node_id)d and visible='t' group by node_id,latitude,longitude""" % {"node_id":row[0]})
			cur.execute("""select node_id,max(version),latitude,longitude from nodes where node_id=%(node_id)d and visible='t' group by node_id,latitude,longitude""" % {"node_id":row[0]})
			result =  cur.fetchone()
			version=result[1]
			node["lat"]=float(result[2])/10**7
			node["lon"]=float(result[3])/10**7

			# Берём теги:
			cur.execute("""select k,v from node_tags where node_id=%(node_id)d and version=%(version)d""" % {"node_id":row[0], "version":version})
			result = cur.fetchall()
			tags={}
			for tag in result:
				tags[tag[0]]=tag[1]
				if tag[0] == "ele":
					try:
						node["ele"]=float(tag[1])
					except Exception:
						node["ele"]=0
			if "ele" in node:
				if node["ele"] != 0:
					if config.debug:
						print("generate_node_list() пропуск node_id=%s, т.к. ele!=0" % node["node_id"])
					continue

			# Пустая точка без обозначений, либо точка, связывающая с подстанцией "для красоты" - не учитываем её:
			if not "ref" in tags:
				continue
			# Бывает не указана высота:
			if not "ele" in node:
				node["ele"]=0
			node["tags"]=tags
			node_list.append(node)
			#print_node(node)
		if len(node_list) > 1:
		#if node_list.length > 1:
			line["node_lists"][way_id]=node_list

def fill_zero_nodes_list(zero_nodes_list,k,v):
	# Берём список идентификаторов точек высота которых равна 0.000000:
	cur.execute("""
		select node_id,max(version) from nodes where visible='t' and (cast(node_id as text) || '-' || cast(version as text) ) in (select (cast(node_id as text) || '-' || cast(max(version) as text) ) as tt from node_tags where k='ele' and v='0.000000' group by node_id) and (cast(node_id as text) || '-' || cast(version as text) ) in (select (cast(node_id as text) || '-' || cast(max(version) as text) ) as tt1 from node_tags where k='%(k)s' and v='%(v)s' group by node_id)  group by node_id;""" % {"k":k, "v":v} )
			
	rows = cur.fetchall()
	for row in rows:
		zero_nodes_list.append(row[0])
	return zero_nodes_list

# ======================================= main() ===========================
tag_find="power"
val_find="tower"
request="line_names"

arguments = cgi.FieldStorage()

for i in arguments.keys():
	if i=='k':
		tag_find=arguments[i].value
	if i=='v':
		val_find=arguments[i].value
	if i=='request':
		request=arguments[i].value

#print "Content-Type: text/html\n\n"; 
print"""
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">
<HTML>
<HEAD>
<META HTTP-EQUIV="CONTENT-TYPE" CONTENT="text/html; charset=utf-8">
<TITLE></TITLE>
<META NAME="GENERATOR" CONTENT="OpenOffice.org 3.1  (Linux)">
<META NAME="AUTHOR" CONTENT="Сергей Семёнов">
<META NAME="CREATED" CONTENT="20100319;10431100">
<META NAME="CHANGEDBY" CONTENT="Сергей Семёнов">
<META NAME="CHANGED" CONTENT="20100319;10441400">
<STYLE TYPE="text/css">
<!--
@page { size: 21cm 29.7cm; margin: 2cm }
P { margin-bottom: 0.21cm }
-->
</STYLE>

<style>
   .normaltext {
   }
</style>
<style>
   .ele_null {
    color: red; /* Красный цвет выделения */
   }
</style>
<style>
   .selected_node {
    color: green; /* Зелёный цвет выделения */
	background: #D9FFAD;
	font-size: 150%;
   }
</style>

</HEAD>
<BODY LANG="ru-RU" LINK="#000080" VLINK="#800000" DIR="LTR">
"""
#print("parameters: %s, node_id_to_find=%s" % (param, node_id_to_find) )

if config.debug:
	print("tag_find=%s, val_find=%s" % (tag_find,val_find))


try:
	if config.debug:
		print("connect to: dbname='" + config.db_name + "' user='" +config.db_user + "' host='" + config.db_host + "' password='" + config.db_passwd + "'")
	conn = psycopg2.connect("dbname='" + config.db_name + "' user='" +config.db_user + "' host='" + config.db_host + "' password='" + config.db_passwd + "'")
	cur = conn.cursor()
except:
    print ("I am unable to connect to the database");sys.exit(1)

lines={}
zero_nodes_list=[]

# Берём список опор, имеющих нулевую высоту:
fill_zero_nodes_list(zero_nodes_list,tag_find,val_find)

for node_id_to_find in zero_nodes_list:
	if config.debug:
		print("DEBUG: main(): proccess node_id=%s" % node_id_to_find)
# Просматриваем отношения:
	if config.debug:
		print("DEBUG: main(): start get_id_of_all_ways_in_relations_for_vl_by_node_id()")
	
	get_id_of_all_ways_in_relations_for_vl_by_node_id(lines,node_id_to_find)
# Добавляем простые линии, если их не добавили как отношения:
	if config.debug:
		print("DEBUG: main(): start get_id_of_all_ways_for_vl_by_node_id()")
	get_id_of_all_ways_for_vl_by_node_id(lines,node_id_to_find)
	#break


for line_name in lines:
	generate_node_list(lines[line_name])
if config.debug:
	print_text_line(lines)
else:
	print_html_line_report(lines)
	print_html_node_report(lines)


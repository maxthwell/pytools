#coding=utf-8
import psycopg2
import time
conn=psycopg2.connect(database="loraserver", user="mfxfxf", password="xxxxxxxxxxx", host="xxx.xxx.xxx.xxx", port="5432")
cur=conn.cursor()
#统计（江山）昨天有今天没的
sql="""with cts_now as (select distinct dev_eui from node_data where dev_eui in (select dev_eui from node where app_eui='\\x0000000000000002') and time_s>now()-interval'1 days'),
	 cts_yest as (select distinct dev_eui from node_data where dev_eui in (select dev_eui from node where app_eui='\\x0000000000000002') and time_s::date<now()-interval'1 days')
	select dev_eui::text,avg(snr::float) snr,avg(rssi::float) rssi from node_data where dev_eui in (select dev_eui from cts_yest except select dev_eui from cts_now) group by dev_eui order by dev_eui;
"""
cur.execute(sql)
js_yestday_except_today=cur.fetchall()
#统计（江山）今天有昨天没的	
sql='''with cts_now as (select distinct dev_eui from node_data where dev_eui in (select dev_eui from node where app_eui='\\x0000000000000002') and time_s>now()-interval'1 days'),
	 cts_yest as (select distinct dev_eui from node_data where dev_eui in (select dev_eui from node where app_eui='\\x0000000000000002') and time_s<now()-interval'1 days')
	select dev_eui::text,avg(snr::float) snr,avg(rssi::float) rssi from node_data where dev_eui in (select dev_eui from cts_now except select dev_eui from cts_yest) group by dev_eui order by dev_eui;
'''
cur.execute(sql)
js_today_except_yestday=cur.fetchall()

#统计（广州烟感）昨天有今天没的
sql='''with cts_now as (select distinct dev_eui from node_data where dev_eui in (select dev_eui from node where app_eui='\\x0000000000000088') and time_s>now()-interval'1 days'),
	 cts_yest as (select distinct dev_eui from node_data where dev_eui in (select dev_eui from node where app_eui='\\x0000000000000088') and time_s<now()-interval'1 days')
	select dev_eui::text,avg(snr::float) snr,avg(rssi::float) rssi from node_data where dev_eui in (select dev_eui from cts_yest except select dev_eui from cts_now) group by dev_eui order by dev_eui;
'''
cur.execute(sql)
gzyg_yestday_except_today=cur.fetchall()

#统计（广州烟感）今天有昨天没的	
sql='''with cts_now as (select distinct dev_eui from node_data where dev_eui in (select dev_eui from node where app_eui='\\x0000000000000088') and time_s>now()-interval'1days'),
	 cts_yest as (select distinct dev_eui from node_data where dev_eui in (select dev_eui from node where app_eui='\\x0000000000000088') and time_s<now()-interval'1 days')
	select dev_eui::text,avg(snr::float) snr,avg(rssi::float) rssi from node_data where dev_eui in (select dev_eui from cts_now except select dev_eui from cts_yest) group by dev_eui order by dev_eui;
'''
cur.execute(sql)
gzyg_today_except_yestday=cur.fetchall()

#查询统计日期信息
sql='''
select substr(max(time_s)::text,1,19) as end_time,substr((max(time_s)-interval'1 days')::text,1,19) as begin_time from node_data;
'''
cur.execute(sql)
time_query=cur.fetchone()

#统计江山燃气的，激活、活跃、不活跃
sql='''
with cts_all as (select no.dev_eui from node no join node_data_static nds on no.dev_eui=nds.dev_eui where nds.up_count>0 and no.app_eui ='\\x0000000000000002'), 
	 cts_active as (select distinct dev_eui from node_data where time_s>now()-interval'1 days' and dev_eui in (select dev_eui from cts_all)),
	 cts_unactive as (select dev_eui from cts_all except select dev_eui from cts_active)
	 select 'all' ,count(*) from cts_all union select 'active',count(*) from cts_active union select 'unactive', count(*) from cts_unactive; 
'''
cur.execute(sql)
js_sta=cur.fetchall()


#统计广州烟感的，激活、活跃、不活跃
sql='''
with cts_all as (select no.dev_eui from node no join node_data_static nds on no.dev_eui=nds.dev_eui where nds.up_count>0 and no.app_eui ='\\x0000000000000088'), 
	 cts_active as (select distinct dev_eui from node_data where time_s>now()-interval'1 days' and dev_eui in (select dev_eui from cts_all)),
	 cts_unactive as (select dev_eui from cts_all except select dev_eui from cts_active)
	 select 'all' ,count(*) from cts_all union select 'active',count(*) from cts_active union select 'unactive', count(*) from cts_unactive; 
'''
cur.execute(sql)
gzyg_sta=cur.fetchall()

#统计(江山)今天未上报数据的模块
sql='''
select dev_eui::text from node_data_static where app_eui ='\\x0000000000000002' and up_count>0 and dev_eui not in (select dev_eui from node_data where time_s>now()-interval'1 days');
'''
cur.execute(sql)
js_noactive_today=cur.fetchall()

#统计(广州烟感)今天未上报数据的模块
sql='''
select dev_eui::text from node_data_static where app_eui ='\\x0000000000000088' and up_count>0 and dev_eui not in (select dev_eui from node_data where time_s>now()-interval'1 days');
'''
cur.execute(sql)
gzyg_noactive_today=cur.fetchall()

#统计（江山）每个模块应该收到多少、实际收到多少、
sql="""with cts_now as (select nd.dev_eui,max(fcnt_up_node::int) mfun, count(*) real_recv, avg(snr::float) snr,avg(rssi::float) rssi from node_data nd join node no on nd.dev_eui=no.dev_eui where no.app_eui='\\x0000000000000002' and nd.time_s>now()-interval'1 days' group by nd.dev_eui),
	cts_yest as (select nd.dev_eui,max(fcnt_up_node::int) mfun, count(*) real_recv, avg(snr::float) snr,avg(rssi::float) rssi from node_data nd join node no on nd.dev_eui=no.dev_eui where no.app_eui='\\x0000000000000002' and nd.time_s<now()-interval'1 days' group by nd.dev_eui)
	select now.dev_eui::text,now.mfun-yest.mfun as exp_recv, now.real_recv,now.mfun-yest.mfun-now.real_recv as lost_recv,now.snr,now.rssi rssi from cts_yest yest join cts_now now on yest.dev_eui=now.dev_eui order by now.dev_eui;
"""
cur.execute(sql)
js_recv_info_today=cur.fetchall()

#统计（广州烟感）每个模块应该收到多少、实际收到多少、
sql="""with cts_now as (select nd.dev_eui,max(fcnt_up_node::int) mfun, count(*) real_recv, avg(snr::float) snr,avg(rssi::float) rssi from node_data nd join node no on nd.dev_eui=no.dev_eui where no.app_eui='\\x0000000000000088' and nd.time_s>now()-interval'1 days' group by nd.dev_eui),
	cts_yest as (select nd.dev_eui,max(fcnt_up_node::int) mfun, count(*) real_recv, avg(snr::float) snr,avg(rssi::float) rssi from node_data nd join node no on nd.dev_eui=no.dev_eui where no.app_eui='\\x0000000000000088' and nd.time_s<now()-interval'1 days' group by nd.dev_eui)
	select now.dev_eui::text,now.mfun-yest.mfun as exp_recv, now.real_recv,now.mfun-yest.mfun-now.real_recv as lost_recv,now.snr,now.rssi rssi from cts_yest yest join cts_now now on yest.dev_eui=now.dev_eui order by now.dev_eui;
"""
cur.execute(sql)
gzyg_recv_info_today=cur.fetchall()

#统计网关在线/离线情况
sql='''with gateway_inuse as (select * from gateway where gw_id in (%s))
       select gw_id,now()<time_s+interval'1 minutes' and age(time_s,bootup_time_s)!='0' as online from gateway_inuse,
'''

def unactove_deveui(): 
	with open('js_noactive_today.txt','w') as f:
		for r in js_noactive_today:
			f.write('%s\n'%r[0])
	with open('gzyg_noactive_today.txt','w') as f:
		for r in gzyg_noactive_today:
			f.write('%s\n'%r[0])

def create_excel():
	import xlwt,time
	wbk=xlwt.Workbook()
	ws=wbk.add_sheet(u'江山燃气表')
	ws.write(0,0,u'模块ID')
	ws.write(0,1,u'应收数据包')
	ws.write(0,2,u'实收数据包')
	ws.write(0,3,u'丢包数量')
	ws.write(0,4,u'平均snr')
	ws.write(0,5,u'平均rssi')
	iRow=0
	for r in js_recv_info_today:
		iRow+=1
		for iCol in range(6):
			ws.write(iRow,iCol,r[iCol])
	ws=wbk.add_sheet(u'广州烟感')
	ws.write(0,0,u'模块ID')
	ws.write(0,1,u'应收数据包')
	ws.write(0,2,u'实收数据包')
	ws.write(0,3,u'丢包数量')
	ws.write(0,4,u'平均snr')
	ws.write(0,5,u'平均rssi')
	iRow=0
	for r in gzyg_recv_info_today:
		iRow+=1
		for iCol in range(6):
			ws.write(iRow,iCol,r[iCol])
	wbk.save(u'module-daily-statistics-%s.xls'%time.strftime("%Y-%m-%d", time.localtime()))	

def content():
	r=time_query
	mail_content='''
		<html>
		hi all：
		<blockquote>
			本封邮件由脚本生成并发送，由于
			时间段为%s至%s
		</blockquote>
	'''%(r[0],r[1])
	sta={}
	for r in js_sta:
		sta[r[1]]=r[0]
	mail_content+='''	
		江山燃气表：
			<blockquote>
			结果如下：
				<blockquote>
				<br>已激活总的模块数量为:%(all)s</br>
				<br>有数据上报模块数量为:%(active)s</br>
				<br>未上报数据模块数量为:%(unactive)s</br>
				</blockquote>
			</blockquote>'''%sta
	mail_content+='''
			<blockquote>
			与昨天对比，新增上报数据的模块：
				<blockquote>
				<table border="1">
				<tr><td>模块ID</td><td>SNR</td><td>RSSI</td></tr>
	'''
	for r in js_today_except_yestday:
		mail_content+='''
				<tr><td>%s</td><td>%s</td><td>%s</td></tr>'''%(r[0],r[1],r[2])
	mail_content+='''
				</table>
				</blockquote>
			</blockquote>
			<blockquote>
			未上报数据模块：
				<blockquote>
				<table border="1">
				<tr><td>模块ID</td><td>SNR</td><td>RSSI</td></tr>
	'''
	for r in js_yestday_except_today:
		mail_content+='''
				<tr><td>%s</td><td>%s</td><td>%s</td></tr>'''%(r[0],r[1],r[2])
	mail_content+='''
				</table>
				</blockquote>
			</blockquote>
	'''
	sta={}
	for r in gzyg_sta:
		sta[r[0]]=r[1]
	mail_content+='''	
		广州烟感：
			<blockquote>
			结果如下：
				<blockquote>
				<br>已激活总的模块数量为:%(all)s</br>
				<br>有数据上报模块数量为:%(active)s</br>
				<br>未上报数据模块数量为:%(unactive)s</br>
				</blockquote>
			</blockquote>'''%sta
	mail_content+='''
			<blockquote>
			与昨天对比，新增上报数据的模块：
				<blockquote>
				<table border="1">
				<tr><td>模块ID</td><td>SNR</td><td>RSSI</td></tr>
	'''
	for r in gzyg_today_except_yestday:
		mail_content+='''
				<tr><td>%s</td><td>%s</td><td>%s</td></tr>'''%(r[0],r[1],r[2])
	mail_content+='''
				</table>
				</blockquote>
			</blockquote>
	'''
	mail_content+='''
			<blockquote>
			未上报数据模块：
				<blockquote>
				<table border="1">
				<tr><td>模块ID</td><td>SNR</td><td>RSSI</td></tr>
	'''
	for r in gzyg_yestday_except_today:
		mail_content+='''
				<tr><td>%s</td><td>%s</td><td>%s</td></tr>'''%(r[0],r[1],r[2])
	mail_content+='''
				</table>
				</blockquote>
			</blockquote>
	'''
	mail_content+='''
	<br>
	本封邮件是由脚本自动生成，其格式是用html控制，如发现有什么缺陷，请及时指正。
	</br>
	'''
	mail_content+='</html>'
	return mail_content

def gateway_offline_event():
	pass
	
def send_email(smtpHost, user, password,sendAddr, recipientAddrs, subject='', content=''):
	import smtplib
	import email.mime.multipart
	import email.mime.text
	from email.mime.text import MIMEText
	from email.mime.multipart import MIMEMultipart
	from email.mime.application import MIMEApplication
	import time
	msg = email.mime.multipart.MIMEMultipart()
	msg['from'] = sendAddr
	msg['to'] = str(recipientAddrs)
	msg['subject'] = str(subject)
	content = content
	txt = email.mime.text.MIMEText(content, 'html', 'utf-8')
	msg.attach(txt)
	# 添加附件，传送D:/软件/yasuo.rar文件
	part = MIMEApplication(open('js_noactive_today.txt','rb').read())
	part.add_header('Content-Disposition', 'attachment', filename=u"未上报数据模块列表江山燃气.txt")
	msg.attach(part)
	part = MIMEApplication(open('gzyg_noactive_today.txt','rb').read())
	part.add_header('Content-Disposition', 'attachment', filename=u"为上报数据模块广州烟感.txt")
	msg.attach(part)
	part = MIMEApplication(open(u'module-daily-statistics-%s.xls'%time.strftime("%Y-%m-%d", time.localtime()),'rb').read())
	part.add_header('Content-Disposition', 'attachment', filename=u"模块上报数据统计.xls")
	msg.attach(part)
	smtp = smtplib.SMTP()
	smtp.connect(smtpHost, '25')
	smtp.login(user, password)
	print('hello world')
	smtp.sendmail(sendAddr, recipientAddrs, str(msg))
	smtp.quit()
	print(u"发送成功！")

if __name__=="__main__":
	unactove_deveui()
	create_excel()
	ct=content()
	try:
		subject = "模块数据上报统计"+time.strftime("%Y-%m-%d", time.localtime())
		print(subject)
		recvs=['ma.xiangxiang@gd-iot.com']#,'gu.qinghuan@gd-iot.com','tu.xiaopeng@gd-iot.com','du.dacai@gd-iot.com','chen.ping@gd-iot.com','zhou.weiming@gd-iot.com']
		send_email('smtp.gd-iot.com', 'ptts@gd-iot.com', 'pingtai123',u'国动信息<ptts@gd-iot.com>', recvs, subject, ct)
	except Exception as err:
		print(err)

	

#!/usr/bin/python2
import os
import getpass
import paramiko

Ip=raw_input("Enter the Ip of the system: " )
Username=raw_input("Enter the Username: ")
password=getpass.getpass("Enter the password of that {0}: ".format(Ip))
junk="<?xml version=\"1.0\"?>\n<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n\n<!-- Put site-specific property overrides in this file. -->\n\n<configuration>\n"
		
while True:
	os.system("tput setaf 4")
	print "\t\t\t\t\tWelcome to Hadoop Installation"
	os.system("tput setaf 0")
	print """
	Press 1 : To install jdk7
	Press 2 : To permanently export java libraries [only 1 time]
	Press 3 : To install hadoop 1.2.1
	Press 4 : To setup HDFS
	Press 5 : To setup MapReduce
	Press 6 : To setup Client
	Press 7 : Exit\n\n"""
	i=raw_input("Enter yor choice: ")
	if int(i)==1:
		os.system("sshpass -p {0} scp /root/Desktop/Project/jdk.rpm {1}:/tmp/".format(password,Ip))
		os.system("sshpass -p {0} ssh {1} rpm -ivh /tmp/jdk.rpm".format(password,Ip))
	 	os.system("sshpass -p {0} ssh {1} export JAVA_HOME=/usr/java/jdk1.7.0_51/".format(password,Ip))
		os.system("sshpass -p {0} ssh {1} export PATH=/usr/java/java1.7.0_51/bin/:$PATH".format(password,Ip))
	elif int(i)==2:
		ssh = paramiko.SSHClient()
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		ssh.connect('{}'.format(Ip), username='{}'.format(Username), password='{}'.format(password))
		ftp = ssh.open_sftp()
		file=ftp.file('/root/.bashrc',"a",-1)
		file.writelines("""export JAVA_HOME=/usr/java/jdk1.7.0_51/
export PATH=/usr/java/java1.7.0_51/bin/:$PATH""")
		file.flush()
		ftp.close()
		ssh.close()
	elif int(i)==3:
		os.system("sshpass -p {0} scp /root/Desktop/Project/hadoop.rpm {1}:/tmp/".format(password,Ip))
		os.system("sshpass -p {0} ssh {1} rpm -ivh /tmp/hadoop.rpm".format(password,Ip))
	elif int(i)==4:
		print """
		press 1: To Setup Namenode
		press 2: To Setup Datanode
		press 3: Exit\n\n"""
		m_IP=raw_input("Enter Master IP: ")
		j=raw_input("Enter Your Choice: ")
		while True:
			if int(j)==1:
				print """\t\t\t\t\t NAMENODE
				press 1: configure namenode settings
				press 2: create master folder
				press 3: format namenode folder
				press 4: firewall off
				press 5: start namenode service
				press 6: check number of datanodes connected
				press 7: exit\n\n"""
				k=raw_input("Enter your choice: ")	
				if int(k)==1:
					ssh = paramiko.SSHClient()
					ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
					ssh.connect('{}'.format(Ip), username='{}'.format(Username), password='{}'.format(password))
					ftp = ssh.open_sftp()
					file=ftp.file('/etc/hadoop/hdfs-site.xml','w')
					file.write("""{0}<property>
<name>dfs.name.dir</name>
<value>/master</value>
</property>

</configuration>""".format(junk))
					file.flush()
					file=ftp.file('/etc/hadoop/core-site.xml','w')
					file.write("""{0}<property>
<name>fs.default.name</name>
<value>hdfs://{1}:9001</value>
</property>

</configuration>""".format(junk,m_IP))	
					file.flush()
					ftp.close()
					ssh.close()		
				elif int(k)==2:
					os.system("sshpass -p {0} ssh {1} mkdir /master".format(password,Ip))	
				elif int(k)==3:
					os.system("sshpass -p {0} ssh {1} hadoop namenode -format -Y".format(password,Ip))
				elif int(k)==4:
					os.system("sshpass -p {0} ssh {1} iptables -F".format(password,Ip))
				elif int(k)==5:
					os.system("sshpass -p {0} ssh {1} hadoop-daemon.sh start namenode".format(password,Ip))
				elif int(k)==6:
					os.system("sshpass -p {0} ssh {1} hadoop dfsadmin -report".format(password,Ip))
				elif int(k)==7:
					break
			elif int(j)==2:
				print """\t\t\t\t\t DATANODE
				press 1: configure datanode settings
				press 2: create datanode folder
				press 3: firewall off
				press 4: start datanode service
				press 5: exit\n\n"""	
				l=raw_input("Enter your Choice: ")
				if int(l)==1:
					ssh = paramiko.SSHClient()
					ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
					ssh.connect('{}'.format(Ip), username='{}'.format(Username), password='{}'.format(password))
					ftp = ssh.open_sftp()
					file=ftp.file('/etc/hadoop/hdfs-site.xml','w')
					file.write("""{0}<property>
<name>dfs.data.dir</name>
<value>/data</value>
</property>

</configuration>""".format(junk))	
					file.flush()
					file=ftp.file('/etc/hadoop/core-site.xml','w')
					file.write("""{0}<property>
<name>fs.default.name</name>
<value>hdfs://{1}:9001</value>
</property>

</configuration>""".format(junk,m_IP))	
					file.flush()
					ftp.close()
					ssh.close()
				elif int(l)==2:
					os.system("sshpass -p {0} ssh {1} mkdir /data".format(password,Ip))
				elif int(l)==3:
					os.system("sshpass -p {0} ssh {1} iptables -F".format(password,Ip))
				elif int(l)==4:
					os.system("sshpass -p {0} ssh {1} hadoop-daemon.sh start datanode".format(password,Ip))
				elif int(l)==5:
					break
			elif int(j)==3:
				break
	elif int(i)==5:
		print """\t\t\t\t\t MAP REDUCE
		press 1: To Setup JobTracker
		press 2: To Setup TaskTracker
		press 3: Exit\n\n"""
		m_IP=raw_input("Enter Master IP: ")
		j_IP=raw_input("Enter Jobtracker IP: ")	
		n=raw_input("Enter your choice: ")
		while True:
			if int(n)==1:
				print """\t\t\t\t\t JOBTRACKER
				press 1: create JobTracker files
				press 2: Turnoff Firewall
				press 4: start JobTracker service
				press 3: check number of Tasktrackers connected
				press 5: exit\n\n"""
				o=raw_input("Enter your choice: ")
				if int(o)==1:
					ssh = paramiko.SSHClient()
					ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
					ssh.connect('{}'.format(Ip), username='{}'.format(Username), password='{}'.format(password))
					ftp = ssh.open_sftp()
					file=ftp.file('/etc/hadoop/mapred-site.xml','w')
					file.write("""{0}<property>
<name>mapred.job.tracker</name>
<value>{1}:9002</value>
</property>

</configuration>""".format(junk,j_IP))	
					file.flush()
					file=ftp.file('/etc/hadoop/core-site.xml','w')
					file.write("""{0}<property>
<name>fs.default.name</name>
<value>hdfs://{1}:9001</value>
</property>

</configuration>""".format(junk,m_IP))	
					file.flush()
					ftp.close()
					ssh.close()
				elif int(o)==2:
					os.system("sshpass -p {0} ssh {1} iptables -F".format(password,Ip))
				elif int(o)==3:
					os.system("sshpass -p {0} ssh {1}hadoop job -list-active-trackers".format(password,Ip))
				elif int(o)==4:
					os.system("sshpass -p {0} ssh {1} hadoop-daemon.sh start jobtracker".format(password,Ip))
				elif int(o)==5:
					break
			elif int(n)==2:
				print """\t\t\t\t\t TASKTRACKER
				press 1: create Tasktracker files
				press 2: firewall off
				press 3: start Tasktracker service
				press 4: exit\n\n"""	
				p=raw_input("Enter your Choice:")
				if int(p)==1:
					ssh = paramiko.SSHClient()
					ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
					ssh.connect('{}'.format(Ip), username='{}'.format(Username), password='{}'.format(password))
					ftp = ssh.open_sftp()
					file=ftp.file('/etc/hadoop/mapred-site.xml','w')
					file.write("""{0}<property>
<name>mapred.job.tracker</name>
<value>{1}:9002</value>
</property>

</configuration>""".format(junk,j_IP))	
					file.flush()
					ftp.close()
					ssh.close()	
				elif int(p)==2:
					os.system("sshpass -p {0} ssh {1} iptables -F".format(password,Ip))
				elif int(p)==3:
					os.system("sshpass -p {0} ssh {1} hadoop-daemon.sh start tasktracker".format(password,Ip))
				elif int(p)==4:
					break
			elif int(n)==3:
				break
	elif int(i)==6:
		print """\t\t\t\t\t CLIENT
		press 1: Configure client settings
		press 2: Check files and folders present in hadoop
		press 3: To make folder in hdfs
		press 4: No. of tasktrackers connected
		press 5: To set up Hive
		press 6: Exit\n\n"""
		m_IP=raw_input("Enter Master IP: ")
		j_IP=raw_input("Enter Jobtracker IP: ")	
		m=raw_input("Enter your Choice: ")
		while True:
			if int(m)==1:
				ssh = paramiko.SSHClient()
				ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
				ssh.connect('{}'.format(Ip), username='{}'.format(Username), password='{}'.format(password))
				ftp = ssh.open_sftp()
				file=ftp.file('/etc/hadoop/core-site.xml','w')
				file.write("""{0}<property>
<name>fs.default.name</name>
<value>hdfs://{1}:9001</value>
</property>

</configuration>""".format(junk,m_IP))	
				file.flush()
				file=ftp.file('/etc/hadoop/mapred-site.xml','w')
				file.write("""{0}<property>
<name>mapred.job.tracker</name>
<value>{1}:9002</value>
</property>

</configuration>""".format(junk,j_IP))	
				file.flush()
				ftp.close()
				ssh.close()
			elif int(m)==2: 
				os.system("sshpass -p {0} ssh {1} hadoop fs -ls /".format(password,Ip))
			elif int(m)==3:
				f=raw_input("Enter folder name: ")
				os.system("hadoop fs -mkdir /{0}".format(f))				
			elif int(m)==4:
				os.system("sshpass -p {0} ssh {1} hadoop job -list-active-trackers".format(password,Ip))
			elif int(m)==5:
				print """\t\t\t\t\t HIVE
				press 1: Install Hive
				press 2: To permanently export hive libraries [only 1 time]
				press 3: To create necessary folders in Hive
				press 4: exit\n\n"""	
				l=raw_input("Enter your Choice: ")
				if int(l)==1:
					os.system("sshpass -p {0} scp /root/Desktop/Project/hive.rpm {1}:/tmp/".format(password,Ip))
					os.system("sshpass -p {0} ssh {1} tar -xvf hive.tar.gz".format(password,Ip))
	 				os.system("sshpass -p {0} ssh {1} export HIVE_HOME=/hive".format(password,Ip))
					os.system("sshpass -p {0} ssh {1} export PATH=/hive/bin/:$PATH".format(password,Ip))
				elif int(l)==2:
					ssh = paramiko.SSHClient()
					ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
					ssh.connect('{}'.format(Ip), username='{}'.format(Username), password='{}'.format(password))
					ftp = ssh.open_sftp()
					file=ftp.file('/root/.bashrc',"a",-1)
					file.writelines("""export HIVE_HOME=/hive
export PATH=/hive/bin/:$PATH""")
					file.flush()
					ftp.close()
					ssh.close()
				elif int(l)==3:
					os.system("sshpass -p {0} ssh {1} hadoop fs -mkdir /user".format(password,Ip))
					os.system("sshpass -p {0} ssh {1} hadoop fs -mkdir /user/hive".format(password,Ip))
					os.system("sshpass -p {0} ssh {1} hadoop fs -mkdir /user/hive/warehouse".format(password,Ip))
					os.system("sshpass -p {0} ssh {1} hadoop fs -chmod 777 /tmp/hive".format(password,Ip))
				elif int(l)==4:
					break
			elif int(m)==6:
				break		
	elif int(i)==7:
		exit()
raw_input("Enter to Continue: ")
os.system("clear")		
raw_input()

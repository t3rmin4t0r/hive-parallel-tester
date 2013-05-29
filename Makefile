
ant-bin.jar:
	wget -c http://archive.apache.org/dist/ant/binaries/apache-ant-1.9.1-bin.tar.gz
	mkdir -p ant/ && tar -C ant/ --strip-components=1 -xzvf apache-ant-1.9.1-bin.tar.gz
	jar -cvf ant-bin.jar ant/

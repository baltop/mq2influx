.PHONY: all install

all: install

install: mq2influx
	scp mq2influx ziumks@ziumdev2.iptime.org:~/mq2influx
	
mq2influx: *.go
	go build .



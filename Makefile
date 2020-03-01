.PHONY: all install

all: install

mq2influx: *.go
	go build .

install: mq2influx
	scp mq2influx ziumks@ziumdev2.iptime.org:~/mq2influx

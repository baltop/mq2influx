FROM scratch
ADD mq2influx /
CMD ["/mq2influx"]

# CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o mq2influx .
# docker build -t mq2influx -f Dockerfile .
# docker run -d --name mq2influx  mq2influx
# docker logs -f --tail 300 mq2influx
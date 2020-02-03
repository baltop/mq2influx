FROM scratch
ADD mq2influx /
CMD ["/mq2influx"]

# CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o mq2cassa .
# docker build -t mq2cassa -f Dockerfile .
# docker run -it --rm mq2cassa
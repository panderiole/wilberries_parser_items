FROM golang:latest
RUN mkdir /app
ADD ./itemsParser/. /app/
WORKDIR /app
RUN go build -o main .
CMD ["/app/main"]

FROM centos:7.6

# 安装Go 1.20
RUN yum install -y wget
RUN wget https://golang.org/dl/go1.20.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.20.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin

WORKDIR /app/gdfs

COPY . /app/gdfs

RUN go mod download

RUN go build cmd/main.go -o dameon

ENTRYPOINT [ "./dameon" ]

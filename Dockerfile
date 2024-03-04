FROM golang:1.20 As buildStage
WORKDIR /src
ADD . /src
RUN cd /src && go build -o main
 
FROM alpine:latest
WORKDIR /tb
COPY --from=buildStage /src/main /tb/
ENTRYPOINT ./main
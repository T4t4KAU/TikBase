FROM golang:1.20 As buildStage
WORKDIR /src
ADD . /src
RUN cd /src && go build -o main
 
FROM alpine:latest
WORKDIR /base
COPY --from=buildStage /src/main /base/
ENTRYPOINT ./main
FROM golang:1.14.2-alpine AS build

RUN apk update && apk add --no-cache git

RUN mkdir -p /usr/schedulo/
WORKDIR /usr/schedulo/

COPY . .
RUN GOMOD111=on CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags='-w -s -extldflags "-static"' -a  -o schedulo_server cmd/schedulo_server/main.go

FROM alpine

RUN mkdir -p /usr/schedulo/
WORKDIR /usr/schedulo/

COPY --from=build /usr/schedulo/schedulo_server ./schedulo_server

ENTRYPOINT ["./schedulo_server"]

EXPOSE 9876
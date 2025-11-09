FROM golang:1.22 AS build
WORKDIR /app
COPY api/go.mod ./
RUN go mod download
COPY api/ .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/server ./

FROM alpine:3.19
RUN adduser -D -h /home/app app
COPY --from=build /out/server /usr/local/bin/server
EXPOSE 8080
USER app
ENTRYPOINT ["/usr/local/bin/server"]


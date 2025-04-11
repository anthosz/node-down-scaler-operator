FROM golang:1.21 AS builder
WORKDIR /app
COPY *go* ./
RUN go mod tidy && go mod download && CGO_ENABLED=0 GOOS=linux go build -a -o node-down-scaler .

FROM alpine:3.18
RUN apk --no-cache add ca-certificates
WORKDIR /srv/
COPY --from=builder /app/node-down-scaler /srv
CMD ["./node-down-scaler"]

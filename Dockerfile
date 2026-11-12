FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /server .

# ---
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /server /server

# No configuration is baked into the image. Mount your own config at runtime:
#   docker run -v $(pwd)/config.json:/etc/starrocks-api/config.json:ro ...
EXPOSE 8081
ENTRYPOINT ["/server"]
CMD ["-config", "/etc/starrocks-api/config.json"]

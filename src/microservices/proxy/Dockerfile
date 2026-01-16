FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o proxy-service ./main.go

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/proxy-service /app/proxy-service
EXPOSE 8000
ENV PORT=8000
CMD ["/app/proxy-service"]

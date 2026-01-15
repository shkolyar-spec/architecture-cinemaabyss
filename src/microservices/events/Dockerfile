FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -mod=mod -o events-service ./main.go

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/events-service /app/events-service
EXPOSE 8082
ENV PORT=8082
CMD ["/app/events-service"]

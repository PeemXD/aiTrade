FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/app ./cmd/app

FROM alpine:3.20

WORKDIR /app
COPY --from=build /out/app /app/app
COPY configs/config.example.env /app/configs/config.example.env
ENTRYPOINT ["/app/app"]

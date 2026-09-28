# syntax=docker/dockerfile:1
ARG GO_VERSION=1.25.14

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/wallet ./cmd/wallet

FROM alpine:3.22
RUN adduser -D -u 10001 wallet
COPY --from=build /out/wallet /usr/local/bin/wallet
USER wallet
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=12 CMD ["wallet", "healthcheck"]
ENTRYPOINT ["wallet"]
CMD ["serve"]

# Build a small static binary; web/ and migrations/ are embedded at compile time.
FROM golang:1.26-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-X main.version=${VERSION}" -o /tracker .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=build /tracker /usr/local/bin/tracker
ENTRYPOINT ["tracker"]

# Build a small static binary; web/ and migrations/ are embedded at compile time.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /tracker .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /tracker /usr/local/bin/tracker
ENTRYPOINT ["tracker"]

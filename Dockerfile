FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/customer-service ./cmd/customer-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/customer-service /customer-service
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/customer-service"]

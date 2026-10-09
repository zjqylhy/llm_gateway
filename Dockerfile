FROM node:24.21.0-bookworm-slim AS web
WORKDIR /src/web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:1.27.1-bookworm AS go
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN for target in gateway admin worker migrate bootstrap cache-init quota-rebuild dev; do \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/$target ./cmd/$target; \
    done

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=go /out/ /app/
COPY --from=web /src/web/dist/ /app/web/dist/
ENV GW_WEB_DIR=/app/web/dist
EXPOSE 8080 8081 8082
USER 65532:65532
CMD ["/app/gateway"]

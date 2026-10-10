# 构建环境可选择官方镜像的其他分发地址，避免依赖单一仓库的网络可用性。
ARG NODE_IMAGE=node:24.21.0-bookworm-slim
ARG GO_IMAGE=golang:1.27.1-bookworm
FROM ${NODE_IMAGE} AS web
WORKDIR /src/web
COPY web/package*.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM ${GO_IMAGE} AS go
ARG GO_PROXY=https://proxy.golang.org,direct
WORKDIR /src
COPY go.mod go.sum ./
RUN GOPROXY="${GO_PROXY}" go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN set -eu; for target in gateway admin worker migrate bootstrap cache-init quota-rebuild dev; do \
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

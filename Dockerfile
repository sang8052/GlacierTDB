# 构建阶段：外网访问走内网代理（HTTP_PROXY/HTTPS_PROXY 由 build-arg 注入），
# Go 模块同时配置 goproxy.cn 兜底。
FROM golang:1.26-alpine AS build
ARG HTTP_PROXY=
ARG HTTPS_PROXY=
ARG NO_PROXY=localhost,127.0.0.1
ENV GOPROXY=https://goproxy.cn,direct \
    CGO_ENABLED=0 \
    GOTOOLCHAIN=auto
WORKDIR /src
COPY . .
RUN go mod tidy && go build -trimpath -ldflags="-s -w" -o /out/glaciertdb .

FROM alpine:3.20
RUN apk add --no-cache tzdata
WORKDIR /app
COPY --from=build /out/glaciertdb /app/glaciertdb
COPY config.yaml /app/config.yaml
EXPOSE 3307
ENTRYPOINT ["/app/glaciertdb"]
CMD ["-config", "/app/config.yaml", "-db-dir", "/data"]

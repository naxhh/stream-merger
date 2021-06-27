FROM golang:1.16 AS build

WORKDIR /src/

COPY . .
RUN go build


FROM debian:10.10-slim
COPY --from=build /src/stream-merger /bin/stream-merger
CMD ["/bin/stream-merger"]

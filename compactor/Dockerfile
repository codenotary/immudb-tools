# Copyright 2022 CodeNotary, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


FROM golang:1.17-alpine as builder
LABEL maintainer="Simone Lazzaris <simone@codenotary.com>"
RUN apk update && apk add --no-cache make
WORKDIR /app
COPY go.* ./
RUN go mod download
COPY Makefile .
COPY *.go .
ARG AES_KEY=zerokey
ARG V_COMMIT=dev
ARG V_VERSION=dev
ARG SKAFFOLD_GO_GCFLAGS
RUN --mount=type=cache,target=/root/.cache/go-build \
    GO_GCFLAGS="${SKAFFOLD_GO_GCFLAGS}" \
    AES_KEY=${AES_KEY} \
    V_COMMIT=${V_COMMIT} \
    V_VERSION=${V_VERSION} \
    make

FROM scratch
ENV GOTRACEBACK=all
WORKDIR /app
COPY --from=builder /app/compactor /app/compactor
ENTRYPOINT ["/app/compactor"]
CMD ["--all", "--oneshot", "-cleanup-percentage", "1", "-flush" ]


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

V_COMMIT ?= $(shell git rev-parse HEAD|head -c 7)
V_BUILT_AT = $(shell date +%s)
V_VERSION ?= devbuild
AES_KEY ?= nokey
IMAGE_TAG ?= replicator
PUSH_TAG ?= immudb-replicator
V_LDFLAGS = -X "main.Version=$(V_VERSION)"\
	-X "main.Buildtime=$(V_BUILT_AT)"\
	-X "main.Commit=$(V_COMMIT)"\
	-X "main.AESKey=$(AES_KEY)"

REPO ?= "codenotary"


replicator: *.go
	CGO_ENABLED=0 go build -o $@ -ldflags '$(V_LDFLAGS)' -gcflags '$(GO_GCFLAGS)'
	
.PHONY: push docker
docker:
	DOCKER_BUILDKIT=1 docker build . -t $(IMAGE_TAG) \
	--label "com.codenotary.commit=$(V_COMMIT)" \
	--build-arg V_VERSION=$(V_VERSION) \
	--build-arg V_COMMIT=$(V_COMMIT) \
	--build-arg AES_KEY=$(AES_KEY) 
        
push: docker
	docker image tag $(IMAGE_TAG):latest $(REPO)/$(PUSH_TAG):$(V_COMMIT)
	docker image tag $(IMAGE_TAG):latest $(REPO)/$(PUSH_TAG):latest
	docker image push $(REPO)/$(PUSH_TAG):$(V_COMMIT)
	docker image push $(REPO)/$(PUSH_TAG):latest

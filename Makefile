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


V_VERSION := 1.3.0
VARS := V_VERSION=$(V_VERSION)

.PHONY: compactor immuguard replicator stresstest
all: compactor immuguard replicator stresstest
compactor:
	 $(VARS) make -C compactor
immuguard:
	$(VARS)  make -C immuguard
replicator:
	$(VARS)  make -C replicator
stresstest:
	$(VARS)  make -C stresstest

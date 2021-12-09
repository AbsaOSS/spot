#
# Copyright 2021 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ARG DOCKER_BASE_ARTIFACT=python
ARG DOCKER_BASE_TAG=3.8-slim-buster

FROM ${DOCKER_BASE_ARTIFACT}:${DOCKER_BASE_TAG}

WORKDIR /app

LABEL \
    vendor="ABSA" \
    copyright="2021 ABSA Group Limited" \
    license="Apache License, version 2.0" \
    name="Spot"

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY ./spot .

ENV PYTHONPATH="/usr/local/bin/python3:/app"

ENV EXTRA_OPTIONS=""

CMD [ "python3", "/app/spot/crawler/crawler.py", "${EXTRA_OPTIONS}" ]

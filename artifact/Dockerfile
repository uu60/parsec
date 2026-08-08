FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    libboost-all-dev \
    libopenmpi-dev \
    libreadline-dev \
    libssl-dev \
    ninja-build \
    openmpi-bin \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /parsec
COPY artifact/requirements.txt /tmp/artifact-requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/artifact-requirements.txt
COPY . /parsec
RUN sh build.sh --O3 --simd=portable

CMD ["./artifact/run.sh", "smoke", "--skip-build", "--comm=tcp"]

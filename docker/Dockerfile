# Dockerfile for Aegean nodes
# Use linux/amd64 because wrk2 may not support default architecture
FROM --platform=linux/amd64 golang:1.23

# Fix bad proxy / caching issues for apt
RUN mkdir -p /etc/apt/apt.conf.d && \
    printf '%s\n' \
      'Acquire::http::Pipeline-Depth 0;' \
      'Acquire::http::No-Cache true;' \
      'Acquire::BrokenProxy true;' \
    > /etc/apt/apt.conf.d/99fixbadproxy

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openssh-server \
    ca-certificates \
    wget \
    git \
    make \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Install oha (prebuilt release binary; supports linux/amd64 + linux/arm64)
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) asset="oha-linux-amd64" ;; \
      arm64) asset="oha-linux-arm64" ;; \
      *) echo "unsupported arch: $arch" >&2; exit 1 ;; \
    esac; \
    wget -O /usr/local/bin/oha "https://github.com/hatoo/oha/releases/latest/download/${asset}"; \
    chmod +x /usr/local/bin/oha; \
    oha --version

# Install system LuaJIT 2.1
RUN apt-get update && apt-get install -y \
    luajit \
    libluajit-5.1-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install wrk2 from source, but use system LuaJIT instead of vendored LuaJIT
# Otherwise, you may get the error: ./luajit: cannot create state: not enough memory
# Because wrk2 hardcodes to use LuaJIT 2.0.5
RUN set -eux; \
    git clone https://github.com/giltene/wrk2.git /tmp/wrk2; \
    cd /tmp/wrk2; \
    cp Makefile Makefile.bak; \
    sed -i 's#^WITH_LUAJIT *=.*#WITH_LUAJIT := /usr#g' Makefile; \
    sed -i 's#^WITH_LUA *=.*#WITH_LUA := /usr#g' Makefile; \
    sed -i 's#deps/luajit/src/libluajit.a#/usr/lib/x86_64-linux-gnu/libluajit-5.1.so#g' Makefile; \
    sed -i "s#cd \$(LDIR) && ./luajit -b#$(command -v luajit) -b#g" Makefile; \
    make; \
    cp /tmp/wrk2/wrk /usr/local/bin/wrk2; \
    chmod +x /usr/local/bin/wrk2; \
    /usr/local/bin/wrk2 --version || true; \
    rm -rf /tmp/wrk2

# Ensure `go` resolves even in shells with a minimal PATH
RUN ln -sf /usr/local/go/bin/go /usr/bin/go

# Configure SSH for passwordless root login
RUN mkdir -p /run/sshd \
    && mkdir -p /root/.ssh \
    && sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PermitEmptyPasswords .*/PermitEmptyPasswords yes/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?UsePAM .*/UsePAM no/' /etc/ssh/sshd_config \
    && ssh-keygen -A

# Make root password empty and ensure account is unlocked
RUN passwd -d root && passwd -u root || true

# Set working directory
WORKDIR /app

# Copy application code
COPY src/ ./src/
COPY experiment/ ./experiment/

# Expose ports
EXPOSE 22
EXPOSE 8000-8100

# Start SSH and keep container running
CMD ["/usr/sbin/sshd", "-D"]
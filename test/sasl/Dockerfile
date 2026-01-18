FROM memcached:latest

USER root

# Install SASL utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends sasl2-bin && \
    rm -rf /var/lib/apt/lists/*

# Create SASL configuration
RUN mkdir -p /etc/sasl2
RUN echo "mech_list: plain" > /etc/sasl2/memcached.conf

# Create SASL user (testuser/testpass)
RUN echo "testpass" | saslpasswd2 -a memcached -c testuser -p

# Set environment for SASL password database
ENV MEMCACHED_SASL_PWDB=/etc/sasldb2

# Run memcached with SASL enabled
CMD ["memcached", "-m", "64", "-vv", "-S", "-u", "root"]

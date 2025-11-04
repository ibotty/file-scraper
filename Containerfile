FROM registry.access.redhat.com/ubi10-minimal
ARG BINARY=target/release/file-scraper

LABEL maintainer="Tobias Florek <tob@butter.sh>"

COPY $BINARY /file-scraper

ENTRYPOINT ["/file-scraper"]
USER 1000

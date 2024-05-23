FROM registry.access.redhat.com/ubi9-minimal
ARG BINARY=target/release/file-scraper

LABEL maintainer="Tobias Florek <tob@butter.sh>"

COPY $BINARY /file-scraper

CMD ["/file-scraper"]
USER 1000

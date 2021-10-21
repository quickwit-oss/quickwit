FROM rustembedded/cross:x86_64-unknown-linux-gnu

COPY linux-image.sh /
RUN /linux-image.sh x86_64

COPY common.sh lib.sh /
RUN /common.sh

COPY cmake.sh /
RUN /cmake.sh

COPY xargo.sh /
RUN /xargo.sh

COPY qemu.sh /
RUN /qemu.sh x86_64 softmmu

COPY dropbear.sh /
RUN /dropbear.sh

COPY --from=0 /qemu /qemu

COPY linux-runner /

ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="/linux-runner x86_64"


RUN yum update -y && \
    yum install perl-core zlib-devel -y && \
    yum install openssl-devel -y && \
    yum install postgresql-devel -y

# RUN apt-get update && \
#     apt-get install -y zlib1g-dev libssl-dev libpq-dev

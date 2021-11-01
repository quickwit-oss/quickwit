set -ex

main() {
    local version=9.6.9
    local triple=$1

    local dependencies=(
        ca-certificates
        curl
        m4
        make
        perl
    )

    # NOTE cross toolchain must be already installed
    apt-get update
    local purge_list=()
    for dep in ${dependencies[@]}; do
        if ! dpkg -L $dep; then
            apt-get install --no-install-recommends -y $dep
            purge_list+=( $dep )
        fi
    done

    td=$(mktemp -d)

    # Download and build libpq form source
    # We first configure the toolchain components.
    # Since we want to cross-compile, we need to make sure the tools being used 
    # to build match the correct architecture (ex: aarch64-linux-gnu-gcc).
    # 
    # After configuring the build, we need to jump into libpq directory since we only need the 
    # Postgres client library. Then build and install libpq.
    # 
    # We also need to build & install `pg_config` because it's invoked when building crates that
    # make use of libpq.
    pushd $td
    curl -kfLO https://ftp.postgresql.org/pub/source/v$version/postgresql-$version.tar.gz
    tar xzf "postgresql-$version.tar.gz" && cd "postgresql-$version" 
    AR=${triple}-ar CC=${triple}-gcc ./configure --host=${triple} \
       --without-readline --without-zlib --without-openssl \
       --prefix=/libpq \
       CFLAGS="-O3 -fPIC"  CXXFLAGS="-fPIC" CPPFLAGS="-fPIC" USE_DEV_URANDOM=1 
    cd src/interfaces/libpq && make all && make install && \
    cd ../../bin/pg_config && make && make install && \
    rm -r /tmp/*

    # clean up
    apt-get purge --auto-remove -y ${purge_list[@]}

    popd

    rm -rf $td
    rm $0
}

main "${@}"

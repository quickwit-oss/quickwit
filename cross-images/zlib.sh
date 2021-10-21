set -ex

main() {
    local version=1.2.11
    local os=$1 \
          triple=$2

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

    

    pushd $td
    curl -k https://zlib.net/zlib-$version.tar.gz | \
        tar --strip-components=1 -xz
    CHOST=arm AR=${triple}ar CC=${triple}gcc ./configure \
      --prefix=/zlib \
    #   no-dso \
    #   $os \
    #   -fPIC \
    #   ${@:3}
    nice make -j$(nproc)
    make install

    # clean up
    apt-get purge --auto-remove -y ${purge_list[@]}

    popd

    rm -rf $td
    rm $0
}

main "${@}"

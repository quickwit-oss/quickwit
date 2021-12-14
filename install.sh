#!/bin/bash

# installer.sh
#
# This is just a little script that can be downloaded from the internet to
# install Quickwit.
# It just does platform detection, fetches the lastest appropriate release version from github
# and execute the appropriate commands to download the binary.
#
# Heavily inspired by the Vector & Meilisearch installation scripts

set -u

# If PACKAGE_ROOT is unset or empty, default it.
PACKAGE_ROOT="${PACKAGE_ROOT:-"https://github.com/quickwit-inc/quickwit/releases/download"}"
PACKAGE_RELEASE_API="${PACKAGE_RELEASE_API:-"https://api.github.com/repos/quickwit-inc/quickwit/releases"}"
PACKAGE_NAME="quickwit"
_divider="--------------------------------------------------------------------------------"
_prompt=">>>"
_indent="   "

header() {
    cat 1>&2 <<EOF

                                   Q U I C K W I T
                                      Installer

$_divider
Website: https://quickwit.io/
Docs: https://quickwit.io/docs/
$_divider

EOF
}

usage() {
    cat 1>&2 <<EOF
quickwit-install
The installer for Quickwit (https://quickwit.io/)

USAGE:
    quickwit-install [FLAGS] [OPTIONS]

FLAGS:
    -h, --help              Prints help information
EOF
}

main() {
    downloader --check
    header
    install_from_archive
}

confirm_install_options() {
    local _version="$1"
    INSTALL_DIR="$HOME/quickwit-${_version}"
    ADD_TO_PATH="Yes"

    echo "Installation directory: ${INSTALL_DIR}"
    echo "Add quickwit to PATH: ${ADD_TO_PATH}"
    read -rp "Do you want to install with these default options (yes/no)? " response </dev/tty
    if echo "$response" | grep -qE "^([Yy][Ee][Ss]|[Yy])$"; 
    then
        return 1
    fi

    read -rp "Enter the installation directory? " response </dev/tty
    if [ ! -z "$response" -a "$response" != " " ];
    then
        INSTALL_DIR="$response"
    fi

    read -rp "Add quickwit to PATH (yes/no)? " response </dev/tty
    if echo "$response" | grep -qE "^([Yy][Ee][Ss]|[Yy])$"; 
    then 
        ADD_TO_PATH="Yes"
    else
        ADD_TO_PATH="No"
    fi

}

install_from_archive() {
    need_cmd cp
    need_cmd mv
    need_cmd rm
    need_cmd tar
    need_cmd chmod
    need_cmd grep
    need_cmd head
    need_cmd sed
    need_cmd curl
    
    get_architecture || return 1
    local _arch="$RETVAL"
    assert_nz "$_arch" "arch"

    local _binary_arch=""
    case "$_arch" in
        # Add `| aarch64-apple-darwin` when M1 is fully supported.
        # Note that M1 binary can still be built from source.
        x86_64-apple-darwin)  
            _binary_arch=$_arch
            ;;
        x86_64-*linux*-gnu)
            _binary_arch="x86_64-unknown-linux-gnu"
            ;;
        x86_64-*linux*-musl)
            _binary_arch="x86_64-unknown-linux-musl"
            ;;
        aarch64-*linux*)
            _binary_arch="aarch64-unknown-linux-musl"
            ;;
	    armv7-*linux*-gnu)
            _binary_arch="armv7-unknown-linux-gnueabihf"
            ;;
	    armv7-*linux*-musl)
            _binary_arch="armv7-unknown-linux-musleabihf"
            ;;
        *)
            printf "%s A pre-built package is not available for your OS architecture: %s" "$_prompt" "$_arch"
            printf "\n"
            err "You can easily build it from source following the docs: https://quickwit.io/docs"
            ;;
    esac

    local _version="$(get_latest_version)"
    confirm_install_options $_version

    local _archive_content_file="quickwit-${_version}-${_binary_arch}"
    local _file="${_archive_content_file}.tar.gz"
    local _archive_content_file_="quickwit-${_version}-${_binary_arch}"
    local _url="${PACKAGE_ROOT}/${_version}/${_file}"

    printf "%s Downloading Quickwit via %s" "$_prompt" "$_url"
    downloader "$_url" "$_file"
    printf "\n"

    printf "%s Unpacking archive ..." "$_prompt"
    tar -xzf "$_file"
    rm "$_file" 
    printf "\n"

    # tar -xzf quickwit-x86_64-unknown-linux-gnu.tar.gz
    # mv asset/* ${INSTALL_DIR}/
    #chmod 744 "${INSTALL_DIR}/bin/${PACKAGE_NAME}"

    # mv "$_archive_content_file" "${PACKAGE_NAME}"
    # chmod 744 "${PACKAGE_NAME}"

    printf "%s Installing binary ..." "$_prompt"
    mkdir -p "${INSTALL_DIR}/bin"
    mv "$_archive_content_file" "${INSTALL_DIR}/bin/${PACKAGE_NAME}"
    chmod 744 "${INSTALL_DIR}/bin/${PACKAGE_NAME}"
    printf "\n"
    
    # create config & logs

    # create tutorials files

    # add binary to path
    if [ "$ADD_TO_PATH" = "Yes" ]; then 
      local _path="export PATH=\"$INSTALL_DIR/bin:\$PATH\""
      printf "\n"
      add_to_path "${HOME}/.zprofile" "${_path}"
      printf "\n"
      add_to_path "${HOME}/.profile" "${_path}"
      printf "\n"
    fi

    printf "\n"
    printf "%s Install succeeded!\n" "$_prompt"
    printf "%s To start using Quickwit:\n" "$_prompt"
    printf "\n"
    printf "%s ./quickwit --version \n" "$_indent"
    printf "\n"
    printf "%s More information at https://quickwit.io/docs/\n" "$_prompt"

    local _retval=$?

    return "$_retval"
}

add_to_path() {
  local file="$1"
  local new_path="$2"

  printf "%s Adding Quickwit path to ${file}" "$_prompt"

  if [ ! -f "$file" ]; then
    echo "${new_path}" >> "${file}"
  else
    grep -qxF "${new_path}" "${file}" || echo "${new_path}" >> "${file}"
  fi
}

# ------------------------------------------------------------------------------
# semverParseInto and semverLT from https://github.com/cloudflare/semver_bash/blob/master/semver.sh
#
# usage: semverParseInto version major minor patch special
# version: the string version
# major, minor, patch, special: will be assigned by the function
# ------------------------------------------------------------------------------

semverParseInto() {
    local RE='[^0-9]*\([0-9]*\)[.]\([0-9]*\)[.]\([0-9]*\)\([0-9A-Za-z-]*\)'
    #MAJOR
    eval $2=`echo $1 | sed -e "s#$RE#\1#"`
    #MINOR
    eval $3=`echo $1 | sed -e "s#$RE#\2#"`
    #PATCH
    eval $4=`echo $1 | sed -e "s#$RE#\3#"`
    #SPECIAL
    eval $5=`echo $1 | sed -e "s#$RE#\4#"`
}

# usage: semverLT version1 version2
semverLT() {
    local MAJOR_A=0
    local MINOR_A=0
    local PATCH_A=0
    local SPECIAL_A=0

    local MAJOR_B=0
    local MINOR_B=0
    local PATCH_B=0
    local SPECIAL_B=0

    semverParseInto $1 MAJOR_A MINOR_A PATCH_A SPECIAL_A
    semverParseInto $2 MAJOR_B MINOR_B PATCH_B SPECIAL_B

    if [ $MAJOR_A -lt $MAJOR_B ]; then
        return 0
    fi
    if [ $MAJOR_A -le $MAJOR_B ] && [ $MINOR_A -lt $MINOR_B ]; then
        return 0
    fi
    if [ $MAJOR_A -le $MAJOR_B ] && [ $MINOR_A -le $MINOR_B ] && [ $PATCH_A -lt $PATCH_B ]; then
        return 0
    fi
    if [ "_$SPECIAL_A"  == "_" ] && [ "_$SPECIAL_B"  == "_" ] ; then
        return 1
    fi
    if [ "_$SPECIAL_A"  == "_" ] && [ "_$SPECIAL_B"  != "_" ] ; then
        return 1
    fi
    if [ "_$SPECIAL_A"  != "_" ] && [ "_$SPECIAL_B"  == "_" ] ; then
        return 0
    fi
    if [ "_$SPECIAL_A" < "_$SPECIAL_B" ]; then
        return 0
    fi

    return 1
}

# Returns the tag of the latest stable release (in terms of semver and not of release date)
get_latest_version() {
    GREP_SEMVER_REGEXP='v\([0-9]*\)[.]\([0-9]*\)[.]\([0-9]*\)$' # i.e. v[number].[number].[number]
    temp_file='temp_file' # temp_file needed because the grep would start before the download is over
    curl -s "${PACKAGE_RELEASE_API}" > "$temp_file" || return 1
    releases=$(cat "$temp_file" | \
        grep -E "tag_name|draft|prerelease" \
        | tr -d ',"' | cut -d ':' -f2 | tr -d ' ')
        # Returns a list of [tag_name draft_boolean prerelease_boolean ...]
        # Ex: v0.10.1 false false v0.9.1-rc.1 false true v0.9.0 false false...

    i=0
    latest=""
    current_tag=""
    for release_info in $releases; do
        if [ $i -eq 0 ]; then # Cheking tag_name
            if echo "$release_info" | grep -q "$GREP_SEMVER_REGEXP"; then # If it's not an alpha or beta release
                current_tag=$release_info
            else
                current_tag=""
            fi
            i=1
        elif [ $i -eq 1 ]; then # Checking draft boolean
            if [ "$release_info" = "true" ]; then
                current_tag=""
            fi
            i=2
        elif [ $i -eq 2 ]; then # Checking prerelease boolean
            if [ "$release_info" = "true" ]; then
                current_tag=""
            fi
            i=0
            if [ "$current_tag" != "" ]; then # If the current_tag is valid
                if [ "$latest" = "" ]; then # If there is no latest yet
                    latest="$current_tag"
                else
                    semverLT $current_tag $latest # Comparing latest and the current tag
                    if [ $? -eq 1 ]; then
                        latest="$current_tag"
                    fi
                fi
            fi
        fi
    done

    rm -f "$temp_file"
    echo $latest
}

# ------------------------------------------------------------------------------
# All code below here was copied from https://sh.rustup.rs and can safely
# be updated if necessary.
# ------------------------------------------------------------------------------

get_gnu_musl_glibc() {
  need_cmd ldd
  need_cmd bc
  need_cmd awk
  # Detect both gnu and musl
  local _ldd_version
  local _glibc_version
  _ldd_version=$(ldd --version)
  if ldd --version 2>&1 | grep -Eq 'GNU'; then
    _glibc_version=$(echo "$_ldd_version" | awk '/ldd/{print $NF}')
    if [ 1 -eq "$(echo "${_glibc_version} < 2.18" | bc)" ]; then
      echo "musl"
    else
      echo "gnu"
    fi
  elif ldd --version 2>&1 | grep -Eq "musl"; then
    echo "musl"
  else
    err "Warning: Unable to detect architecture from ldd (using gnu-unknown)"
  fi
}

get_bitness() {
    need_cmd head
    # Architecture detection without dependencies beyond coreutils.
    # ELF files start out "\x7fELF", and the following byte is
    #   0x01 for 32-bit and
    #   0x02 for 64-bit.
    # The printf builtin on some shells like dash only supports octal
    # escape sequences, so we use those.
    local _current_exe_head
    _current_exe_head=$(head -c 5 /proc/self/exe )
    if [ "$_current_exe_head" = "$(printf '\177ELF\001')" ]; then
        echo 32
    elif [ "$_current_exe_head" = "$(printf '\177ELF\002')" ]; then
        echo 64
    else
        err "unknown platform bitness"
    fi
}

get_endianness() {
    local cputype=$1
    local suffix_eb=$2
    local suffix_el=$3

    # detect endianness without od/hexdump, like get_bitness() does.
    need_cmd head
    need_cmd tail

    local _current_exe_endianness
    _current_exe_endianness="$(head -c 6 /proc/self/exe | tail -c 1)"
    if [ "$_current_exe_endianness" = "$(printf '\001')" ]; then
        echo "${cputype}${suffix_el}"
    elif [ "$_current_exe_endianness" = "$(printf '\002')" ]; then
        echo "${cputype}${suffix_eb}"
    else
        err "unknown platform endianness"
    fi
}

get_architecture() {
    local _ostype _cputype _bitness _arch
    _ostype="$(uname -s)"
    _cputype="$(uname -m)"

    if [ "$_ostype" = Linux ]; then
        if [ "$(uname -o)" = Android ]; then
            _ostype=Android
        fi
    fi

    if [ "$_ostype" = Darwin ] && [ "$_cputype" = i386 ]; then
        # Darwin `uname -m` lies
        if sysctl hw.optional.x86_64 | grep -q ': 1'; then
            _cputype=x86_64
        fi
    fi

    case "$_ostype" in

        Android)
            _ostype=linux-android
            ;;

        Linux)
            case $(get_gnu_musl_glibc) in
              "musl")
                _ostype=unknown-linux-musl
                ;;
              "gnu")
                _ostype=unknown-linux-gnu
                ;;
              # Fallback
              *)
                _ostype=unknown-linux-gnu
                ;;
            esac
            _bitness=$(get_bitness)
            ;;

        FreeBSD)
            _ostype=unknown-freebsd
            ;;

        NetBSD)
            _ostype=unknown-netbsd
            ;;

        DragonFly)
            _ostype=unknown-dragonfly
            ;;

        Darwin)
            _ostype=apple-darwin
            ;;

        MINGW* | MSYS* | CYGWIN*)
            _ostype=pc-windows-gnu
            ;;

        *)
            err "unrecognized OS type: $_ostype"
            ;;

    esac

    case "$_cputype" in

        i386 | i486 | i686 | i786 | x86)
            _cputype=i686
            ;;

        xscale | arm)
            _cputype=arm
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            fi
            ;;

        armv6l)
            _cputype=arm
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            else
                _ostype="${_ostype}eabihf"
            fi
            ;;

        armv7l | armv8l)
            _cputype=armv7
            if [ "$_ostype" = "linux-android" ]; then
                _ostype=linux-androideabi
            else
                _ostype="${_ostype}eabihf"
            fi
            ;;

        aarch64 | arm64)
            _cputype=aarch64
            ;;

        x86_64 | x86-64 | x64 | amd64)
            _cputype=x86_64
            ;;

        mips)
            _cputype=$(get_endianness mips '' el)
            ;;

        mips64)
            if [ "$_bitness" -eq 64 ]; then
                # only n64 ABI is supported for now
                _ostype="${_ostype}abi64"
                _cputype=$(get_endianness mips64 '' el)
            fi
            ;;

        ppc)
            _cputype=powerpc
            ;;

        ppc64)
            _cputype=powerpc64
            ;;

        ppc64le)
            _cputype=powerpc64le
            ;;

        s390x)
            _cputype=s390x
            ;;

        *)
            err "unknown CPU type: $_cputype"

    esac

    # Detect 64-bit linux with 32-bit userland
    if [ "${_ostype}" = unknown-linux-gnu ] && [ "${_bitness}" -eq 32 ]; then
        case $_cputype in
            x86_64)
                _cputype=i686
                ;;
            mips64)
                _cputype=$(get_endianness mips '' el)
                ;;
            powerpc64)
                _cputype=powerpc
                ;;
            aarch64)
                _cputype=armv7
                if [ "$_ostype" = "linux-android" ]; then
                    _ostype=linux-androideabi
                else
                    _ostype="${_ostype}eabihf"
                fi
                ;;
        esac
    fi

    # Detect armv7 but without the CPU features Rust needs in that build,
    # and fall back to arm.
    # See https://github.com/rust-lang/rustup.rs/issues/587.
    if [ "$_ostype" = "unknown-linux-gnueabihf" ] && [ "$_cputype" = armv7 ]; then
        if ensure grep '^Features' /proc/cpuinfo | grep -q -v neon; then
            # At least one processor does not have NEON.
            _cputype=arm
        fi
    fi

    _arch="${_cputype}-${_ostype}"

    RETVAL="$_arch"
}

err() {
    echo "$_prompt $1" >&2
    exit 1
}

need_cmd() {
    if ! check_cmd "$1"; then
        err "need '$1' (command not found)"
    fi
}

check_cmd() {
    command -v "$1" > /dev/null 2>&1
}

assert_nz() {
    if [ -z "$1" ]; then err "assert_nz $2"; fi
}

# Run a command that should never fail. If the command fails execution
# will immediately terminate with an error showing the failing
# command.
ensure() {
    local output
    output="$("$@" 2>&1 > /dev/null)"

    if [ "$output" ]; then
        echo ""
        echo "$_prompt command failed: $*"
        echo ""
        echo "$_divider"
        echo ""
        echo "$output" >&2
        exit 1
    fi
}

# This is just for indicating that commands' results are being
# intentionally ignored. Usually, because it's being executed
# as part of error handling.
ignore() {
    "$@"
}

# This wraps curl or wget. Try curl first, if not installed,
# use wget instead.
downloader() {
    if [ "$1" = --check ]; then
        need_cmd curl
    else
        if ! check_help_for curl --proto --tlsv1.2; then
            echo "Warning: Not forcing TLS v1.2, this is potentially less secure"
            curl --silent --show-error --fail --location "$1" --output "$2"
        else
            curl --proto '=https' --tlsv1.2 --silent --show-error --fail --location "$1" --output "$2"
        fi
    fi
}

check_help_for() {
    local _cmd
    local _arg
    local _ok
    _cmd="$1"
    _ok="y"
    shift

    # If we're running on OS-X, older than 10.13, then we always
    # fail to find these options to force fallback
    if check_cmd sw_vers; then
        if [ "$(sw_vers -productVersion | cut -d. -f2)" -lt 13 ]; then
            # Older than 10.13
            echo "Warning: Detected OS X platform older than 10.13"
            _ok="n"
        fi
    fi

    for _arg in "$@"; do
        if ! "$_cmd" --help | grep -q -- "$_arg"; then
            _ok="n"
        fi
    done

    test "$_ok" = "y"
}

main "$@" || exit 1

# Sourced only by offline bootstrap tests. No command accesses a real device.
event() { printf '%s\n' "$*" >>"$TEST_ROOT/events"; }
id() { (( MOCK_USER_EXISTS )); }
useradd() { event useradd; MOCK_USER_EXISTS=1; }
apt-get() { event "apt-get $*"; [[ "$MOCK_APT_FAIL" != 1 ]]; }
apt-cache() { [[ "$MOCK_PERF_AVAILABLE" == 1 ]]; }
runuser() { event rustup; shift 3; "$@"; }
install() {
    local arg
    for arg in "$@"; do
        [[ "$arg" != "$TEST_ROOT"* ]] || mkdir -p "$arg"
    done
}
chown() { event "chown $*"; }
find() {
    local index
    for ((index = 0; index < MOCK_LINK_COUNT; index++)); do
        printf '/dev/disk/by-id/google-local-nvme-ssd-%s\n' "$((index + MOCK_BAD_NAME * 99))"
    done
}
readlink() { local link="${@: -1}"; printf '/dev/mock-ssd-%s\n' "${link##*-}"; }
test() {
    if [[ "$1" != -b ]]; then builtin test "$@"; return; fi
    if [[ "$2" == /dev/md/test-ssd ]]; then
        (( MOCK_ARRAY_PRESENT ))
    else
        (( ! MOCK_MISSING_DEVICE ))
    fi
}
blockdev() {
    if (( MOCK_SMALL_DEVICE )); then printf '1\n'; else printf '402653184000\n'; fi
}
mdadm() {
    case "$1" in
        --assemble) (( MOCK_REASSEMBLE )) || return 1; MOCK_ARRAY_PRESENT=1; event assembled ;;
        --examine) (( MOCK_RAID_SIGNATURE )) ;;
        --create) MOCK_ARRAY_PRESENT=1; event raid-created ;;
        --detail) printf 'ARRAY /dev/md/test-ssd UUID=mock-array\n' ;;
        *) return 99 ;;
    esac
}
udevadm() { :; }
update-initramfs() { :; }
wipefs() {
    if [[ "${@: -1}" == /dev/md/test-ssd ]]; then
        printf '%s' "$MOCK_ARRAY_SIGNATURE"
    elif (( MOCK_ARRAY_PRESENT )); then
        printf 'linux_raid_member'
    else
        printf '%s' "$MOCK_RAW_SIGNATURE"
    fi
}
blkid() {
    if [[ " $* " == *" TYPE "* ]]; then
        printf '%s' "$MOCK_FILESYSTEM"
    else
        printf 'mock-filesystem-uuid\n'
    fi
}
mkfs.ext4() { event "formatted ${@: -1}"; MOCK_FILESYSTEM=ext4; }
mountpoint() { (( MOCK_MOUNTED )); }
mount() { event mounted; MOCK_MOUNTED=1; }
findmnt() { printf '%s\n' "${MOCK_MOUNTED_UUID:-mock-filesystem-uuid}"; }

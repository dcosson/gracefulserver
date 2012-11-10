// Code to deal with keeping a file descriptor open so we can reconnect to it
// next time we launch (even if we launch a new binary).  Taken, like much of
// this graceful restart code, from
// http://code.google.com/p/jra-go/source/browse/cmd/upgradable/upgradable.go
package gracefulserver

import (
    "fmt"
    "net"
    "syscall"
)

// These are here because there is no API in syscall for turning OFF
// close-on-exec (yet).
//
// from syscall/zsyscall_linux_386.go, but it seems like it might work
// for other platforms too.
func fcntl(fd int, cmd int, arg int) (val int, err error) {
        r0, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(cmd), uintptr(arg))
        val = int(r0)
        if e1 != 0 {
                err = e1
        }
        return
}

func noCloseOnExec(fd uintptr) {
        fcntl(int(fd), syscall.F_SETFD, ^syscall.FD_CLOEXEC)
}

// Prevent the filedescriptor attached to a TCPListener from closing
func noCloseTCPListener(l *net.TCPListener) (uintptr, error) {
    fdObj, err := l.File()
    fd := fdObj.Fd()
    if err != nil {
        fmt.Printf("FD error for fd: %d (ignoring)\n", fd)
    }
    noCloseOnExec(fd)
    return fd, nil
}
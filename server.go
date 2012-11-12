package gracefulserver

import (
    "fmt"
    // "time"
    "errors"
    "os"
    "os/signal"
    "syscall"
    "net"
    "net/http"
    "sync"
)

var (
    ServerConnWaitGroup = sync.WaitGroup{}
    relaunchFnLocal func(uintptr) error
    debugMode = true
)

// A ListenAndServe function for running an http server that shuts down
// gracefully (stops taking new connections and waits until all existing
// connectiosn are closed before exiting).
// It can also optionally restart itself on the same file descriptor so no
// connections will drop during the restart, but note that (AFAIK) the only way
// to do this is by calling a start command with os/exec, i.e. starting as a
// completely new process, which will likely confuse your process monitor.
//
// In between accepting connections, the listener checks for a stop message on
// a specified channel, at which point Serve() will return so we're no longer
// accepting new connections. We then wait for existing connections to close
// before returning
func ListenAndServe(addr string, handler http.Handler,
        fd uintptr, relaunchFn func(uintptr) error) (err error) {
    simplelog("In listen and serve")
    // set up server and listener
    server := &http.Server{Addr: addr}
    listener, err := getStoppableListener(fd, server.Addr)
    if err != nil {
        return err
    }
    relaunchFnLocal = relaunchFn

    // Handle signals
    ch := make(chan os.Signal, 1)
    signal.Notify(ch, syscall.SIGHUP)
    signal.Notify(ch, syscall.SIGTERM)
    go signalHandler(ch, listener)

    // Serve content
    err = server.Serve(listener)
    simplelog("Server exited, waiting on connections...")
    // Wait until remaining connections are closed
    ServerConnWaitGroup.Wait()
    return
}

// The signal type is just used to send a notification through a channel
type signalStruct struct{}

// Errors to return when the listener stops
// error to return when the listener stops
// type ErrorWithFD struct {
//     error
//     fd int
// }

var Stopped = errors.New("stopped gracefully")
var Restarted = errors.New("restarted gracefully")

// a connection that we're watching to decrement the connection when it 
type watchedConn struct {
    net.Conn
}

func (wc watchedConn) Close() error {
    simplelog("closed a connection")
    err := wc.Conn.Close()
    simplelog("-- wait group")
    ServerConnWaitGroup.Done()
    return err
}

// create a watchedConn type with the connection, which will decrement the
// connection counter when it is closed
func startWatchingConn(c net.Conn) (watchedConn) {
    ServerConnWaitGroup.Add(1)
    simplelog("++ waitgroup")
    return watchedConn{Conn: c}
}

//
// network Listeners
//

// wrapper around a net.Listener that holds a stop channel on which we can
// signal it to shut down gracefully
type stoppableListener struct {
    net.Listener 
    underlyingFile *os.File
    stop chan signalStruct
    stopForRestart chan signalStruct
}

// Gets the File from a stoppable Listener.  Assumes it can be cast as a
// TCPListener
func (l *stoppableListener) File() (f *os.File, err error) {
    if l.underlyingFile != nil {
        simplelog("getting stoppableListener's file, already attached")
        f = l.underlyingFile
    } else {
        simplelog("attaching file copy to stoppableListener")
        f, err = l.Listener.(*net.TCPListener).File()
    }
    return
}

// Create a new stoppableListener from a regular Listener
func upgradeListener(l net.Listener) *stoppableListener {
    return &stoppableListener{
        Listener: l,
        stop: make(chan signalStruct, 1),
        stopForRestart: make(chan signalStruct, 1),
    }
}

// Get a new stoppableListener, either by creating a new TCPListener or if a
// valid fd is specified opening a TCP listener on that file descriptor 
func getStoppableListener(fd uintptr, addr string) (sl *stoppableListener, err error) {
    var listener net.Listener
    var f *os.File
    // listen on already open file
    if fd != 0 {  
        simplelogf("Opening file listener on fd: %d\n", fd)
        f = os.NewFile(uintptr(fd), "listen socket")
        listener, err = net.FileListener(f)
        if err != nil {
            simplelogf("Couldn't open File listener on fd: %d, falling back " +
                "to new tcp listener\n", fd)
        }
    }
    // if we failed or no file descriptor was given, create new tcp connection
    if fd == 0 || err != nil {  // create new tcp listener
        simplelog("Opening a new tcp listener")
        listener, err = net.Listen("tcp", addr)
        if err != nil {
            simplelog("Couldn't get new tcp listener!")
            return
        }
    }
    sl = upgradeListener(listener)
    // if we started from an open file, attach it to the listener
    sl.underlyingFile = f
    return
}

// This wrapper for net.Listener's Accept() method does the "heavy lifting" for
// shutting down gracefully.  It listens on the stoppableListener's stop
// channel and when it gets a signal it returns the Stopped error, which will
// cause Serve to return.
//
// This method also converts every incoming connection to a watchedConnection,
// which is using a waitgroup under the hood to keep track of how many
// connections (server requests) are still open.  
func (l *stoppableListener) Accept() (c net.Conn, err error) {
    select {
    case <-l.stopForRestart:
        simplelog("received stopForRestart signal")
        // Keep FD open
        fd, fdErr := noCloseTCPListener(l)
        if fdErr != nil {
            panic("Error getting listener's descriptor: " + fdErr.Error())
        }
        // Relaunch the app, using the function specified to do so
        simplelogf("Relaunching on file descriptor: %d\n", fd)
        relaunchErr := relaunchFnLocal(fd)
        if relaunchErr == nil {
            simplelog("Relaunched successfully")
        } else {
            simplelogf("Relaunch error: '%v'\n", relaunchErr)
        }
        // close underlying file, if available
        if l.underlyingFile != nil {
            fileErr := l.underlyingFile.Close()
            if fileErr != nil {
                simplelogf("Error closing stoppableListener's underlying " +
                    "file: %s\n", fileErr.Error())
            }
        }
        err = Restarted

    case <-l.stop:
        simplelog("received stop signal")
        // still keep FD open so we can reattach
        fd, fdErr := noCloseTCPListener(l)
        if fdErr != nil {
            panic("Error getting listener's descriptor: " + fdErr.Error())
        }
        simplelogf("Exiting, but leaving fd %d open to reattach to\n", fd)
        err = Stopped

    default:
        simplelog("Waiting for request in stoppableListener's Accept()...")
        c, err = l.Listener.Accept()
        simplelog("opened a connection")
        if err != nil {
            simplelog("error in underlying tcp listener's Accept()")
            return
        }
        c = startWatchingConn(c)
    }
    simplelog("Returning from stoppableListener's Accept()")
    return
}

func (l *stoppableListener) Close() error {
    simplelog("Called Close() on the stoppableListener")
    return l.Listener.Close()
}

//
// Handle OS Signals
//

// Listen for signals from the OS and send message to stop server
func signalHandler(ch <-chan os.Signal, l *stoppableListener) {
    signal := <-ch
    if signal == syscall.SIGHUP {
        simplelog("Caught SIGHUP, restarting after next request...")
        l.stopForRestart <- signalStruct{}
    } else if signal == syscall.SIGTERM {
        simplelog("Caught SIGTERM, stopping after next request...")
        l.stop <- signalStruct{}
    }
}

// Simple log wrappers that only log if debugging is on
func simplelogf(s string, args... interface{}) {
    if debugMode {
        fmt.Printf(s, args)
    }
}
func simplelog(s string) {
    if debugMode {
        fmt.Println(s)
    }
}

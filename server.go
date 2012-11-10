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
        relaunchFn func(uintptr) error) (err error) {
    fmt.Println("In listen and serve")
    // set up server and listener
    server := &http.Server{Addr: addr}
    listener, listenerErr := net.Listen("tcp", server.Addr)
    if listenerErr != nil {
        fmt.Println("Couldn't get tcp listener!")
        return listenerErr
    }
    csListener := upgradeListener(listener)

    // Handle signals
    ch := make(chan os.Signal, 1)
    signal.Notify(ch, syscall.SIGHUP)
    go signalHandler(ch, csListener)

    // Serve content
    err = server.Serve(csListener)
    fmt.Println("Server exited, waiting on connections...")
    // Wait until remaining connections are closed
    ServerConnWaitGroup.Wait()

    if err == Stopped {
        fmt.Println("Serve returned with 'Stopped'")
        // re-launch app on same file descriptor
        fd, fdErr := noCloseTCPListener(csListener.Listener.(*net.TCPListener))
        if fdErr != nil {
            panic("Error getting listener's descriptor: " + fdErr.Error())
        }
        fmt.Printf("Relaunching on file descriptor: %d\n", fd)
        relaunchErr := relaunchFn(fd)
        if relaunchErr == nil {
            fmt.Println("Relaunched successfully")
        } else {
            fmt.Printf("Relaunch error: '%v'\n", relaunchErr)
        }
    }
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

var Stopped = errors.New("listener stopped")

type stoppableListener struct {
    net.Listener 
    stopForRestart chan signalStruct
}

// a connection that we're watching to decrement the connection when it 
type watchedConn struct {
    net.Conn
}

func (wc watchedConn) Close() error {
    fmt.Println("closed a connection")
    err := wc.Conn.Close()
    fmt.Println("-- wait group")
    ServerConnWaitGroup.Done()
    return err
}

// create a watchedConn type with the connection, which will decrement the
// connection counter when it is closed
func startWatchingConn(c net.Conn) (watchedConn) {
    ServerConnWaitGroup.Add(1)
    fmt.Println("++ waitgroup")
    return watchedConn{Conn: c}
}


// Create a new stoppableListener from a regular Listener
func upgradeListener(l net.Listener) *stoppableListener {
    return &stoppableListener{
        Listener: l,
        stopForRestart: make(chan signalStruct, 1),
    }
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
        fmt.Println("received stop signal")
        err = Stopped
        return
    //TODO: case l.stop
    default:

        fmt.Println("Waiting for request in stoppableListener's Accept()...")
        c, err = l.Listener.Accept()
        fmt.Println("opened a connection")
        if err != nil {
            fmt.Println("Caught error in underlying tcp listener's Accept()")
            return
        }
        c = startWatchingConn(c)
    }
    fmt.Println("Returning from stoppableListener's Accept()")
    return
}

// Listen for signals from the OS and send message to stop server
func signalHandler(ch <-chan os.Signal, l *stoppableListener) {
    <-ch
    fmt.Println("Caught signal, stopping after next request...")
    l.stopForRestart <- signalStruct{}
}

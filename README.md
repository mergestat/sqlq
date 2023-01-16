# sqlq

> :warning: still very experimental! proceed with care!

**`sqlq`** is a SQL-backed, persistent, job queuing solution.

## Usage

To start using `sqlq` add it to your dependencies with:

```shell
go get -u github.com/mergestat/sqlq
```

### Enqueing new jobs

Using the [`sqlq.Enqueue()`](https://pkg.go.dev/github.com/mergestat/sqlq#sqlq.Enqueue) function, push a job to a queue:

```golang
func NewSendMail(args ...string) *sqlq.JobDescription {
    var params = args // somehow serialize args ... using json.Encode() probably
    return sqlq.NewJobDesc("send-email", WithParameters(params));
}


var conn *sql.DB // handle to an open database connection

job, err := sqlq.Enqueue(conn, "default", NewSendMail(from, to, body))
if err != nil {
    // handle error here
}
```

### De-queuing and processing jobs

To de-queue and process a job, you need to use one of the available runtimes. Currently, the only available runtime is the `embed` runtime
available under `github.com/mergestat/sqlq/runtime/embed` package. The following example demonstrates how to implement a job handler for that runtime.

```golang
func SendMail() sqlq.Handlerfunc {
    return func(ctx context.Context, job *sqlq.Job) error { 
        // send mail's implementation
        return nil
    }
}

func main() {
    var upstream *sql.DB // handle to an open database connection
    var worker, _ = embed.NewWorker(upstream, embed.WorkerConfig{
		    Queues: []sqlq.Queue{"embed/default"}, // queues to watch
    })
    
    _ = worker.Register("send-mail", SendMail())
    
    // starting the worker will start the processing routine in background
    if err := worker.Start(); err != nil {
        // handle error
    }
    
    // wait here .. probably listening for os.Signal
    
    // make a clear / graceful exit
    if err := worker.Shutdown(5 * time.Second); err != nil {
        // graceful exit failed / didn't complete
        return nil
    }
}

```

These basic examples are just to give an idea. Refer to tests to see more varied use cases.

## Developing

To start a `postgres` database in Docker and execute tests against it, run:

```sh
docker-compose up
```

and then:

```sh
go test ./... -postgres-url postgres://postgres:password@localhost:5432 -v
```

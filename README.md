# sqlq

> :warning: still very experimental!

**`sqlq`** is a SQL-backed, persistent, job queuing solution.


## Developing

To start a `postgres` database in Docker and execute tests against it, run:

```sh
docker-compose up
```

and then:

```sh
go test ./... -postgres-url postgres://postgres:password@localhost:5432 -v
```

package sqlq_test

import (
	"database/sql"
	"flag"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/mergestat/sqlq/schema"
	"log"
	"os"
	"testing"
)

// PostgresUrl is the url to the upstream postgresql instance passed in as flag to 'go test'
var PostgresUrl string

func init() {
	flag.StringVar(&PostgresUrl, "postgres-url", "postgres://localhost/mergestat", "url to the test database")
}

func TestMain(m *testing.M) {
	flag.Parse()

	var upstream = MustOpen(PostgresUrl)
	defer upstream.Close()

	// bring up the sql migrations
	if err := schema.Apply(upstream); err != nil {
		log.Fatalf("failed to apply schema migration: %v", err)
	}

	// run all test cases
	var code = m.Run()

	// teardown the sql migrations
	// if err := schema.Teardown(upstream); err != nil {
	//	 log.Fatalf("failed to teardown schema migration: %v", err)
	// }

	os.Exit(code)
}

func MustOpen(url string) *sql.DB {
	upstream, err := sql.Open("pgx", url)
	if err != nil {
		log.Fatalf("failed to open connection to upstream: %v", err)
	}
	return upstream
}

package postgres

import (
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func New(connStr string) *sqlx.DB {
	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		log.Fatalln(err)
	}
	return db
}

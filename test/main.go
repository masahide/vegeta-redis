package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {

	var (
		idle = 5
		open = 5
		dsn  = "bench:cygames0@tcp(10.9.10.223:3307)/hayao10_hayao"
	)
	flag.IntVar(&idle, "idle", idle, "SetMaxIdleConns")
	flag.IntVar(&open, "open", open, "SetMaxOpenConns")
	flag.StringVar(&dsn, "dsn", dsn, "dsn")
	flag.Parse()

	cnn, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer cnn.Close()
	log.Printf("open:%v", open)
	log.Printf("idle:%v", idle)
	cnn.SetMaxIdleConns(idle)
	cnn.SetMaxOpenConns(open)

	for i := 0; i < 100; i++ {
		for j := 0; j < 1000; j++ {
			go func() {
				rows, err := cnn.Query("SELECT rand()")
				if err != nil {
					log.Fatal(err)
				}
				defer rows.Close()
				for rows.Next() {
					var id interface{}
					if err := rows.Scan(&id); err != nil {
						log.Fatal(err)
					}
					fmt.Print(".")
				}

				if err := rows.Err(); err != nil {
					log.Fatal(err)
				}
			}()
		}
		time.Sleep(1 * time.Second)
	}

}

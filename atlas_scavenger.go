package main

import (
	"encoding/json"
	_ "github.com/lib/pq"
	"log"
	"flag"
	"net/http"
	"strconv"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"os"
)

func main() {
	var atlasobjs interface{}
	var idx uint64
	var runCount uint
	var maxRunCount uint
	var userName string
	var passWord string
	var host string
	var dbName string
	var connectString string
	var checkTable bool
	numberDone := make(chan struct{})

	flag.Uint64Var(&idx, "start_id", 1000000, "specify starting measurement id from atlas API.  defaults to 1000000 (1 million).")
	flag.StringVar(&userName, "username", "", "specify username to connect to database (required)")
	flag.StringVar(&passWord, "password", "", "specify password to connect to database (required)")
	flag.StringVar(&host, "host", "127.0.0.1", "specify hostname of the server where the database is running")
	flag.StringVar(&dbName, "dbname", "atlas-msm", "specify name of the database where the measurements will be stored.")
	flag.UintVar(&maxRunCount, "number_of_runs", 4000, "specify the maximum number of separate rest queries (500 measurements per query). Defaults to 4000.")
	var resume = flag.Bool("resume", false, "resume from the last measurement in database")
	flag.Parse()

	if (*resume == true) && (idx != 1000000) {
		log.Fatal(`Ambiguous command, cannot resume with a specified starting id.
		Use either -resume or -start-atlas-id, but not both.`)
	}
	if userName == "" {
		log.Fatal(`Need username to connect to database (with --username).`)
	}

	connectString = "user=" + userName + " password=" + passWord + " dbname=" + dbName + " host=" + host + " sslmode=disable"
	db, err := sql.Open("postgres", connectString)
	db.SetMaxOpenConns(10)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if (*resume == false && idx == 1000000) {
		fmt.Println("deleting table meas...")
		db.Exec("TRUNCATE meas")
	}

	query := "SELECT EXISTS (SELECT * FROM pg_tables WHERE tablename='meas');"
	db.QueryRow(query).Scan(&checkTable)

	if checkTable == false {
		fmt.Println("creating table `meas`...")
		query := `CREATE TABLE "public"."meas" (
			"body" jsonb NOT NULL,
			"id" serial NOT NULL PRIMARY KEY
		)`
		_, err = db.Exec(query)
		if err != nil {
			log.Fatal(err)
		}
	}

	if (*resume == true) {
		query := `SELECT MAX((body#>>'{id}')::INT) FROM meas;`
		var resumeIdx string
		err = db.QueryRow(query).Scan(&resumeIdx)
		fmt.Println("resuming from measurement " + resumeIdx + "...")
		if err != nil {
			log.Fatal(err)
		}
		idx, err = strconv.ParseUint(resumeIdx, 10, 64)
	}


	for runCount < maxRunCount {

		getString := "https://atlas.ripe.net/api/v2/measurements?id__gt=" + strconv.FormatUint(idx, 10) + "&page_size=500"
		log.Print(getString)
		timeout := time.Duration(30 * time.Second)
		httpClient := http.Client{
			Timeout: timeout,
		}
		res, err := httpClient.Get(getString)
		if err != nil {
			log.Fatal(err)
		}

		defer res.Body.Close()

		decoder := json.NewDecoder(res.Body)
		decoder.UseNumber()
		err = decoder.Decode(&atlasobjs)
		if err != nil {
			log.Print(err)
		}

		measMap := atlasobjs.(map[string]interface{})["results"].([]interface{})
		measCount := len(measMap)

		if measCount == 0 {
			fmt.Println("No measurements found from this id onwards. exiting...")
			os.Exit(0)
		}

		runCount++
		idx, _ = strconv.ParseUint(string(measMap[len(measMap) - 1].(map[string]interface{})["id"].(json.Number)), 10, 64)

		go func(measMap []interface{}) {

			log.Print("run nr :\t", runCount)
			log.Print("entries count :\t", measCount)
			log.Printf("last id :\t%v", idx)

			for msm := range measMap {
				body, _ := json.Marshal(measMap[msm])
				bodyString := strings.Replace(string(body[:len(body)]), "'", "''", -1)
				query := fmt.Sprintf("INSERT INTO meas (body) VALUES ('%s');\n", bodyString)
				_, err := db.Exec(query)
				if err != nil {
					log.Print(query)
					log.Fatal(err)
				}
				//defer row.Close()
			}

			if len(measMap) < 500 {
				// apparantly the last measurements
				close(numberDone)
				db.Close()
				fmt.Println("Closed database and exiting....")
				os.Exit(0)
			}

			numberDone <- struct{}{}
		}(measMap)

		<-numberDone
	}

}




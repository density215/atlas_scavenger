package main

import (
	"encoding/json"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"strconv"
	"database/sql"
	//"fmt"
	"fmt"
)

func main() {
	var atlasobjs interface{}
	var idx uint64
	resultsReady := make(chan []interface{})
	done := make(chan struct{})

	db, err := sql.Open("postgres", "user=jasper dbname=jasper sslmode=disable")
	db.Exec("TRUNCATE meas")
	db.SetMaxOpenConns(10)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	go func(uint64) {
		getString := "https://atlas.ripe.net/api/v2/measurements?id__gte=" + strconv.FormatUint(idx, 10) + "&page_size=500"
		log.Print(getString)
		res, err := http.Get(getString)
		if err != nil {
			log.Print(err)
		}

		defer res.Body.Close()

		decoder := json.NewDecoder(res.Body)
		decoder.UseNumber()
		err = decoder.Decode(&atlasobjs)
		if err != nil {
			log.Print(err)
		}

		measMap := atlasobjs.(map[string]interface{})["results"].([]interface{})
		count := len(measMap)
		idx, _ = strconv.ParseUint(string(measMap[len(measMap) - 1].(map[string]interface{})["id"].(json.Number)), 10, 64)

		log.Print("entries count :\t", count)
		log.Printf("last id :\t%v", idx)
		resultsReady <- measMap

	}(idx)

	go func() {
		results := <-resultsReady
		log.Print("done receiving ", len(results), " results.")

		for msm := range results {
			body, _ := json.Marshal(results[msm])
			query := fmt.Sprintf("INSERT INTO meas (body) VALUES ('%s');\n", body)
			_, err := db.Exec(query)
			if err != nil {
				log.Fatal(err)
			}
			//defer row.Close()
		}
		done <- struct{}{}
	}()

	<-done
}




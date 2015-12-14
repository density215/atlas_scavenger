package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
)

func main() {
	var atlasobjs interface{}
	var idx uint64

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
	results, err := json.Marshal(measMap)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("number of bytes received\t: ", len(results))
	log.Print("entries count :\t", count)
	log.Printf("last id :\t%v", idx)

}




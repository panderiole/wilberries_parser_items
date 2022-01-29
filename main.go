package main

import (
	"database/sql"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/getsentry/sentry-go"
	"github.com/lib/pq"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Id struct {
	id       int
	category string
}

const connStr = "user=postgres password=991155 dbname=wilberries sslmode=disable"

func GetDbIds() []Id {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	res, _ := db.Query("Select id, category from items")
	var ids []Id
	for res.Next() {
		id := Id{}
		res.Scan(&id.id, &id.category)
		ids = append(ids, id)
	}
	return ids
}

func updateItemInfoPostgreSql(id int, priceF float64, salePriceF float64, colors []string, sizes []string, count int, category string) int {
	dt := time.Now() //
	db, err := sql.Open("postgres", connStr)
	rand.Seed(time.Now().UnixNano())
	//_, _ := strconv.ParseFloat(strconv.Itoa(rand.Intn(2))+"."+strconv.Itoa(rand.Intn(90-10)+10), 64)

	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec("update items set infodate = array_append(infodate, $1), count = $2, prices = array_append(prices, $3), saleprices = array_append(saleprices, $4), colors = $5, sizes = $6, category = $7 where id = $8",
		dt.Format("01-02-2006"), count, priceF, salePriceF, pq.Array(colors), pq.Array(sizes), category, id)
	if err, ok := err.(*pq.Error); ok {
		fmt.Println("pq error:", err.Code.Name())
		time.Sleep(time.Second * 2)
		return updateItemInfoPostgreSql(id, priceF, salePriceF, colors, sizes, count, category)

	}
	return 1
}

func scrapItem(id string, category string) int {
	url := "https://wbxcatalog-ru.wildberries.ru/nm-2-card/catalog?locale=ru&nm=" + id
	res, err := http.Get(url)
	if err != nil {
		time.Sleep(time.Second * 3)
		return scrapItem(id, category)
	}
	body, e := ioutil.ReadAll(res.Body)
	if e != nil {
		time.Sleep(time.Second * 3)
		return scrapItem(id, category)
	}
	c, _, _, _ := jsonparser.Get(body, "data", "products")
	_, err = jsonparser.ArrayEach(c, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		price, _, _, error1 := jsonparser.Get(value, "priceU")
		salePrice, _, _, error2 := jsonparser.Get(value, "salePriceU")

		colorsObj, _, _, _ := jsonparser.Get(value, "colors")
		sizeObj, _, _, _ := jsonparser.Get(value, "sizes")
		var colors, sizes []string
		count := 0
		_, err1 := jsonparser.ArrayEach(colorsObj, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			color, _, _, e1 := jsonparser.Get(value, "name")
			if e1 == nil {
				colors = append(colors, string(color))
			}
		})
		if err1 != nil {
			return
		}
		_, err2 := jsonparser.ArrayEach(sizeObj, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			size, _, _, e1 := jsonparser.Get(value, "name")
			if e1 == nil {
				sizes = append(sizes, string(size))
			}
			stockObj, _, _, _ := jsonparser.Get(value, "stocks")
			_, err3 := jsonparser.ArrayEach(stockObj, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
				cc, _, _, e1 := jsonparser.Get(value, "qty")
				if e1 == nil {
					ccInt, _ := strconv.Atoi(string(cc))
					count += ccInt
				}
			})
			if err3 != nil {
				return
			}
		})
		if err2 != nil {
			return
		}
		if error1 != nil && error2 != nil {
			return
		}
		var priceF, salePriceF float64
		if error1 != nil {
			salePriceS := string(salePrice)
			salePriceS = salePriceS[:len(salePriceS)-2] + "." + salePriceS[len(salePriceS)-2:]
			salePriceF, _ = strconv.ParseFloat(salePriceS, 8)
			priceF = salePriceF
		} else if error2 != nil {
			priceS := string(price)
			priceS = priceS[:len(priceS)-2] + "." + priceS[len(priceS)-2:]
			priceF, _ = strconv.ParseFloat(priceS, 8)
			salePriceF = priceF
		} else {
			priceS := string(price)
			salePriceS := string(salePrice)
			priceS = priceS[:len(priceS)-2] + "." + priceS[len(priceS)-2:]
			salePriceS = salePriceS[:len(salePriceS)-2] + "." + salePriceS[len(salePriceS)-2:]
			priceF, _ = strconv.ParseFloat(priceS, 64)
			salePriceF, _ = strconv.ParseFloat(salePriceS, 64)
		}
		idInt, _ := strconv.Atoi(id)
		updateItemInfoPostgreSql(idInt, priceF, salePriceF, colors, sizes, count, category)
	})
	if err != nil {
		time.Sleep(time.Second * 3)
		return scrapItem(id, category)
	}
	return 1
}

func scrapItems() {
	var wg sync.WaitGroup
	err := sentry.Init(sentry.ClientOptions{
		Dsn: "https://f20597c3014e4699969af0244a66a6f8@o1108001.ingest.sentry.io/6135375",
	})
	if err != nil {
		log.Fatalf("sentry.Init: %s", err)
	}
	defer sentry.Flush(2 * time.Second)
	fmt.Println("[4/4] Скрипт парсера товаров запущен!")
	count := 0
	data := GetDbIds()
	for i, v := range data {
		count += 1
		wg.Add(1)
		go func(id int, category string) {
			defer wg.Done()
			scrapItem(strconv.Itoa(id), category)
		}(v.id, v.category)
		if i%50 == 0 {
			wg.Wait()
			if i%5000 == 0 {
				fmt.Println("[4/4] Обработано " + strconv.Itoa(count) + " из " + strconv.Itoa(len(data)))
			}

		}
	}
	wg.Wait()
	fmt.Println("[4/4] Скрипт парсера товаров завершен!")
}

func main() {
	for {
		scrapItems()
		fmt.Println("FINISH")
		os.Exit(0)
	}
}

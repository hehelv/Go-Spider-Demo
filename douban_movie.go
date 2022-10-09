package main

import (
	"database/sql"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"log"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const (
	USERNAME = "root"
	PASSWORD = ""
	HOST = "127.0.0.1"
	PORT = "3306"
	DBNAME = "douban_movie"
)

var DB *sql.DB
var wg sync.WaitGroup

type MovieData struct {
	Title string
	Director string
	Picture string
	Actor string
	Year string
	Score string
	Quote string
}

func main() {
	InitDB()
	ch := make(chan bool)
	for i := 0; i < 10; i++ {
		//wg.Add(1)
		go Spider(strconv.Itoa(i*25), ch)
	}

	for i := 1; i <= 10; i++ {
		<-ch
	}
	//wg.Wait()
}

func Spider(page string, ch chan bool) {
	// 1. 发送请求
	client := &http.Client{}

	req, err := http.NewRequest("GET", "https://movie.douban.com/top250?start=" + page, nil)
	if err != nil {
		log.Fatalf("http.NewRequest failed, err: %v", err)
	}

	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Cache-Control", "max-age=0")
	req.Header.Set("sec-ch-ua-mobile", "?0")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Referer", "https://movie.douban.com/chart")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("client.Do failed, err: %v", err)
	}
	defer resp.Body.Close()
	// 2. 解析网页
	dom, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		log.Fatalf("goquery.NewDocumentFromReader failed, err: %v", err)
	}

	// 3. 获取节点信息, 通过正则表达式解析相关数据
	dom.Find("#content > div > div.article > ol > li").Each(func(i int, s *goquery.Selection) {
		var movieData MovieData
		// 提取数据
		title := s.Find("div.info > div.hd > a > span:nth-child(1)").Text()
		img := s.Find("div > div.pic > a > img")
		imgSrc, ok := img.Attr("src")
		content := s.Find("div > div.info > div.bd > p:nth-child(1)").Text()
		info := strings.Trim(content, " ")
		director, actor, year := InitInfo(info)
		score := strings.Trim(s.Find("div > div.info > div.bd > div > span.rating_num").Text(), " ")
		score = strings.Trim(score, "\n")
		quote := strings.Trim(s.Find("div.info > div.bd > p.quote > span").Text(), " ")
		if ok {
			movieData.Title = title
			movieData.Picture = imgSrc
			movieData.Director = director
			movieData.Actor = actor
			movieData.Year = year
			movieData.Score = score
			movieData.Quote = quote
			fmt.Println(movieData)
			// 4. 保存信息
			Insert(movieData)
		}
	})
	//wg.Done()
	ch <- true
}


func InitInfo(info string) (director, actor, year string) {
	directorRe, _ := regexp.Compile(`导演:(.*)主演:`)
	director = string(directorRe.Find([]byte(info)))
	director = strings.Trim(director, "主演:")
	actorRe, _ := regexp.Compile(`主演:(.*)`)
	actor = string(actorRe.Find([]byte(info)))
	yearRe, _ := regexp.Compile(`(\d+)`)
	year = string(yearRe.Find([]byte(info)))
	return
}

func InitDB() {
	path := strings.Join([]string{USERNAME, ":", PASSWORD, "@tcp(", HOST, ":", PORT, ")/", DBNAME, "?charset=utf8"}, "")
	DB, _ = sql.Open("mysql", path)
	DB.SetConnMaxLifetime(10)
	DB.SetMaxIdleConns(5)
	if err := DB.Ping(); err != nil{
		fmt.Println("opon database fail")
		return
	}
	fmt.Println("connnect success")
}

func Insert(movieData MovieData) {
	tx, err := DB.Begin()
	if err != nil {
		fmt.Println("tx fail")
		return
	}
	stmt, err := tx.Prepare("INSERT INTO movie_data (`Title`,`Director`,`Picture`,`Actor`,`Year`,`Score`,`Quote`) VALUES (?, ?, ?,?,?,?,?)")
	if err != nil {
		fmt.Println("Prepare fail", err)
		return
	}
	_, err = stmt.Exec(movieData.Title, movieData.Director, movieData.Picture, movieData.Actor, movieData.Year, movieData.Score, movieData.Quote)
	if err != nil {
		fmt.Println("Exec fail", err)
		return
	}
	_ = tx.Commit()
	return
}

/*
CREATE TABLE IF NOT EXISTS `movie_data`(E
   `id` INT UNSIGNED AUTO_INCREMENT,
   `Title` VARCHAR(100) NOT NULL,
   `Director` VARCHAR(100),
   `Picture` VARCHAR(100),
   `Actor` VARCHAR(40),
   `Year` VARCHAR(40),
   `Score` VARCHAR(40),
   `Quote` VARCHAR(40),
   PRIMARY KEY ( `id` )
)ENGINE=InnoDB DEFAULT CHARSET=utf8;
*/
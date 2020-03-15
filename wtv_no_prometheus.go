package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Wtv struct {
	TemplateId []string `yaml:"templateid"`
	GetAllDayUrl string `yaml:"getallday_url"`
	GetChannelsUrl string `yaml:"getchannels_url"`
	DB string `yaml:"db"`
}

type Program struct {
	StartTime string `json:"StartTime"`
	EndTime string `json:"EndTime"`
	UrlType string `json:"UrlType"`
}

type allPrograms struct {
	PlayDate string `json:"playDate"`
	//Programs []map[string]interface{} `json:"programs"`
	Programs []Program `json:"programs"`
}

type wtvData struct {
	id string // 直播模版号
	uuid []string // 直播uuid
}

func (w *wtvData) VerifyData(url string, done func(), wtvChan chan <- map[string]map[string]int) {
	defer done()
	resId := make(map[string]map[string]int)
	resUuid := make(map[string]int)
	for _, v := range w.uuid {
		p := httpAllDay(url + "?" + "templateId=" + w.id + "&" + "uuid=" + v)
		if len(p) == 0 {
			fmt.Printf("%s %s programs null\n", w.id, v)
			resUuid[v] = 1
			resId[w.id] = resUuid
			wtvChan <- resId
		}
		zeroTime := getTime()
		programTime, _ := strconv.ParseInt(p[0].PlayDate,10,64)
		if programTime >= zeroTime {
			var futureTime []string
			var playTime []string
			var trueFutureTime []string
			content0 := p[0].Programs
			for _, v := range content0 {
				switch v.UrlType {
				case "none":
					futureTime = append(futureTime, v.EndTime, v.StartTime)
				case "play":
					playTime = append(playTime, v.EndTime)
				case "replay":
				default :
					fmt.Println("[0]没有匹配到")
				}
			}
			if len(playTime) == 0 {
				content1 := p[1].Programs
				for _, v := range content1 {
					switch v.UrlType {
					case "none":
						futureTime = append(futureTime, v.EndTime, v.StartTime)
					case "play":
						playTime = append(playTime, v.EndTime)
					case "replay":
					default:
						fmt.Println("[1]没有匹配到")
					}
				}
			}
			// 23:30的直播信息在p[1], 第一个预加载节目单可能也在 所以要遍历p[0] p[1]
			if len(playTime) == 0 {
				fmt.Printf("%s %s programs now miss\n", w.id, v)
				resUuid[v] = 2
				resId[w.id] = resUuid
				//continue
			}
			// 有的频道节目单回看urlType为空，要去掉这些数据
			for _, value := range futureTime {
				if value >= playTime[0] {
					trueFutureTime = append(trueFutureTime, value)
				}
			}
			switch len(trueFutureTime) {
			case 0:
				fmt.Printf("%s %s programs future miss\n", w.id, v)
				resUuid[v] = 4
				resId[w.id] = resUuid
			case 1,2,3:
				fmt.Printf("%s %s programs future less\n", w.id, v)
				resUuid[v] = 5
				resId[w.id] = resUuid
			case 4:
				if playTime[0] == trueFutureTime[3] && trueFutureTime[2] == trueFutureTime[1] {
					fmt.Printf("%s %s programs ok\n", w.id, v)
					resUuid[v] = 0
					resId[w.id] = resUuid
				} else if playTime[0] < trueFutureTime[3] || trueFutureTime[2] < trueFutureTime[1] {
					fmt.Printf("%s %s programs future uncontinuity\n", w.id, v)
					resUuid[v] = 6
					resId[w.id] = resUuid
				} else {
					fmt.Printf("%s %s programs future overlap\n", w.id, v)
					resUuid[v] = 7
					resId[w.id] = resUuid
				}
			}
		} else {
			fmt.Printf("%s %s programs today miss\n", w.id, v)
			resUuid[v] = 2
			resId[w.id] = resUuid
		}
	}
}

func ParseYaml(path string) *Wtv {
	config, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Println("error=", err)
	}
	var setting Wtv
	yaml.Unmarshal(config, &setting)
	return &setting
}

func VerifyDb() int8 {
	setting := ParseYaml("./wtvConfig.yaml")
	db, err := sql.Open("sqlite3", setting.DB)
	defer db.Close()
	if err != nil {
		fmt.Printf("连接数据库失败 %v", err)
		return 1
	}
	var wg sync.WaitGroup
	var wtvChan chan map[string]map[string]int
	wtvChan = make(chan map[string]map[string]int)
	for _, v := range setting.TemplateId {
		wg.Add(1)
		rows, err := db.Query("select uuid from w" + v)
		if err != nil {
			fmt.Printf("查询失败 err=%v\n", err)
		}
		var uuidOffline []string
		for rows.Next(){
			var s string
			err = rows.Scan(&s)
			if err !=nil{
				fmt.Println(err)
			}
			uuidOffline = append(uuidOffline, s)
		}
		rows.Close()
		var wd *wtvData = &wtvData{v, uuidOffline}
		go wd.VerifyData(setting.GetAllDayUrl, wg.Done, wtvChan)
	}
	wg.Wait()
	return 0
}

// 获取每天凌晨的时间戳
func getTime() int64 {
	t := time.Now()
	zeroTime := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix()
	return zeroTime
}

// 获取接口数据并反序列化
func httpAllDay(url string) []allPrograms {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch: reading %s: %v\n", url, err)
		os.Exit(1)
	}
	//fmt.Printf("%s", b)
	var slice []allPrograms
	err = json.Unmarshal(b, &slice)
	if err != nil {
		fmt.Printf("unmarshal err=%v\n", err)
	}
	//if len(slice) == 0 {
	//	fmt.Fprint(os.Stderr, "programs null")
	//	os.Exit(1)
	//}
	return slice
}

func main() {
	VerifyDb()
}
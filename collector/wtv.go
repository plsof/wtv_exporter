package collector

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// 指标结构体
type Metrics struct {
	templateId string
	metrics map[string]*prometheus.Desc
}

/**
 * 函数：newGlobalMetric
 * 功能：创建指标描述符
 */
func newGlobalMetric(namespace string, metricName string, docString string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName(namespace, metricName, "metric"),
		docString,
		[]string{"templateId", "uuid"}, nil)
}

/**
 * 工厂方法：NewMetrics
 * 功能：初始化指标信息，即Metrics结构体
 */
func NewMetrics(namespace string) *Metrics {
	return &Metrics{
		metrics: map[string]*prometheus.Desc{
			"uuid_analysis_metric": newGlobalMetric(namespace, "uuid_analysis", "The description of uuid_analysis_metric"),
		},
	}
}

/**
 * 接口：Describe
 * 功能：传递结构体中的指标描述符到channel
 */
func (c *Metrics) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range c.metrics {
		ch <- m
	}
}

/**
 * 接口：Collect
 * 功能：抓取最新的数据，传递给channel
 */
func (c *Metrics) Collect(ch chan<- prometheus.Metric) {
	setting := ParseYaml("./wtvConfig.yaml")
	db, err := sql.Open("sqlite3", setting.DB)
	defer db.Close()
	if err != nil {
		fmt.Printf("连接数据库失败 %v", err)
		os.Exit(1)
	}
	var wg sync.WaitGroup
	for _, id := range setting.TemplateId {
		rows, err := db.Query("select uuid from w" + id)
		if err != nil {
			fmt.Printf("查询失败 err=%v\n", err)
		}
		for rows.Next(){
			var uuid string
			err = rows.Scan(&uuid)
			if err !=nil{
				fmt.Println(err)
			}
			wg.Add(1)
			go VerifyData(setting.GetAllDayUrl, id, uuid, wg.Done, ch)
		}
		rows.Close()
	}
	wg.Wait()
}

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

func VerifyData(url string, id string, uuid string, done func(), ch chan<- prometheus.Metric) {
	defer done()
	p := httpAllDay(url + "?" + "templateId=" + id + "&" + "uuid=" + uuid)
	if len(p) == 0 {
		fmt.Printf("%s %s programs null\n", id, uuid)
		ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(1), id, uuid)
		return
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
			fmt.Printf("%s %s programs now miss\n", id, uuid)
			ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(2), id, uuid)
			return
		}
		// 有的频道节目单回看urlType为空，要去掉这些数据
		for _, value := range futureTime {
			if value >= playTime[0] {
				trueFutureTime = append(trueFutureTime, value)
			}
		}
		switch len(trueFutureTime) {
		case 0:
			fmt.Printf("%s %s programs future miss\n", id, uuid)
			ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(3), id, uuid)
		case 1,2,3:
			fmt.Printf("%s %s programs future less\n", id, uuid)
			ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(4), id, uuid)
		case 4:
			if playTime[0] == trueFutureTime[3] && trueFutureTime[2] == trueFutureTime[1] {
				fmt.Printf("%s %s programs ok\n", id, uuid)
				ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(0), id, uuid)
			} else if playTime[0] < trueFutureTime[3] || trueFutureTime[2] < trueFutureTime[1] {
				fmt.Printf("%s %s programs future uncontinuity\n", id, uuid)
				ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(5), id, uuid)
			} else {
				fmt.Printf("%s %s programs future overlap\n", id, uuid)
				ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(6), id, uuid)
			}
		}
	} else {
		fmt.Printf("%s %s programs today miss\n", id, uuid)
		ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(2), id, uuid)
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
	return slice
}
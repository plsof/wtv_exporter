package collector

import (
	"database/sql"
	"encoding/json"
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"wtv_exporter/customlogger"
)

// 指标结构体
type Metrics struct {
	metrics *prometheus.Desc
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
		newGlobalMetric(namespace, "uuid_analysis", "The description of uuid_analysis_metric"),
	}
}

/**
 * 接口：Describe
 * 功能：传递结构体中的指标描述符到channel
 */
func (c *Metrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.metrics
}

/**
 * 接口：Collect
 * 功能：抓取最新的数据，传递给channel
 */
func (c *Metrics) Collect(ch chan<- prometheus.Metric) {
	logger := customlogger.GetInstance()
	//清空文件内容
	os.Truncate(logger.Filename, 0)
	logger.Onfile.Seek(0, 0)
	//解析配置文件
	setting := parseYaml()
	db, err := sql.Open("sqlite3", setting.DB)
	defer db.Close()
	if err != nil {
		logger.Fatalln("数据库连接失败, err=", err)
	}
	// 获取每天凌晨的时间戳
	zeroTime := func () int64 {
		t := time.Now()
		zeroTime := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Unix()
		return zeroTime
	}()
	var wg sync.WaitGroup
	var uuid string
	for _, id := range setting.TemplateId {
		//获取getChannels接口数据并反序列化
		channels := func () []Channel {
			resp, err := http.Get(setting.GetChannelsUrl + "?" + "templateId=" + id)
			if err != nil {
				logger.Fatalln("获取getChannels接口失败 err=", err)
			}
			defer resp.Body.Close()
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				logger.Fatalln("读取getChannels接口内容失败 err=", err)
			}
			var slice []Channel
			err = json.Unmarshal(b, &slice)
			if err != nil {
				logger.Fatalln("getChannels接口反序列化失败 err=", err)
			}
			return slice
		}()
		rows, err := db.Query("select uuid from w" + id)
		if err != nil {
			logger.Println("查询失败 err=", err)
		}
		for rows.Next(){
			err = rows.Scan(&uuid)
			if err !=nil{
				logger.Println("获取uuid失败 err=", err)
			}
			var offset int8
			offset = 0
			for _, c := range channels {
				if uuid == c.Uuid {
					offset +=1
				}
			}
			wg.Add(1)
			go VerifyData(setting.GetAllDayUrl, id, uuid, offset, zeroTime, wg.Done, ch)
		}
		rows.Close()
	}
	wg.Wait()
}

type Wtv struct {
	TemplateId []string `yaml:"templateid"`
	GetChannelsUrl string `yaml:"getchannels_url"`
	GetAllDayUrl string `yaml:"getallday_url"`
	DB string `yaml:"db"`
	LogPath string `yaml:"logpath"`
}

type Channel struct {
	Uuid string `json:uuid`
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

func parseYaml() *Wtv {
	logger := customlogger.GetInstance()
	config, err := ioutil.ReadFile("./wtvConfig.yaml")
	if err != nil {
		logger.Println("解析配置yaml文件出错 err=", err)
	}
	var setting Wtv
	yaml.Unmarshal(config, &setting)
	return &setting
}

func VerifyData(url string, id string, uuid string, offset int8, zeroTime int64, done func(), ch chan<- prometheus.Metric) {
	defer done()
	logger := customlogger.GetInstance()
	if offset == 0 {
		logger.Printf("%s %s programs lack\n", id, uuid)
		ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(-1), id, uuid)
		return
	}
	// 获取getAllDayPrograms接口数据并反序列化
	p := func () []allPrograms {
		resp, err := http.Get(url + "?" + "templateId=" + id + "&" + "uuid=" + uuid)
		if err != nil {
			logger.Fatalln("获取getAllDayPrograms接口失败 err=", err)
		}
		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logger.Fatalln("读取getAllDayPrograms接口内容失败 err=", err)
		}
		var slice []allPrograms
		err = json.Unmarshal(b, &slice)
		if err != nil {
			logger.Fatalln("getAllDayPrograms接口反序列化失败 err=", err)
		}
		return slice
	}()
	if len(p) == 0 {
		logger.Printf("%s %s programs null\n", id, uuid)
		ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(1), id, uuid)
		return
	}
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
				logger.Println("[0]没有匹配到")
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
					logger.Println("[1]没有匹配到")
				}
			}
		}
		// 23:30的直播信息在p[1], 第一个预加载节目单可能也在 所以要遍历p[0] p[1]
		if len(playTime) == 0 {
			logger.Printf("%s %s programs now miss\n", id, uuid)
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
			logger.Printf("%s %s programs future miss\n", id, uuid)
			ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(3), id, uuid)
		case 1,2,3:
			logger.Printf("%s %s programs future less\n", id, uuid)
			ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(4), id, uuid)
		case 4:
			if playTime[0] == trueFutureTime[3] && trueFutureTime[2] == trueFutureTime[1] {
				logger.Printf("%s %s programs ok\n", id, uuid)
				ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(0), id, uuid)
			} else if playTime[0] < trueFutureTime[3] || trueFutureTime[2] < trueFutureTime[1] {
				logger.Printf("%s %s programs future uncontinuity\n", id, uuid)
				ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(5), id, uuid)
			} else {
				logger.Printf("%s %s programs future overlap\n", id, uuid)
				ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(6), id, uuid)
			}
		}
	} else {
		logger.Printf("%s %s programs today miss\n", id, uuid)
		ch <-prometheus.MustNewConstMetric(newGlobalMetric("wtv", "uuid_analysis", "The description of uuid_analysis_metric"), prometheus.GaugeValue, float64(7), id, uuid)
	}
}






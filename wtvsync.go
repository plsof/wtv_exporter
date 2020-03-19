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
	"wtv_db/customlogger"
)

type Wtv struct {
	TemplateId []string `yaml:"templateid"`
	IgnoreUuid []string `yaml:"ignore_uuid"`
	GetChannelsUrl string `yaml:"getchannels_url"`
	DB string `yaml:"db"`
}

type Channel struct {
	Uuid string `json:uuid`
	No int64 `json:no`
}

//解析配置文件
func parseYaml() *Wtv {
	logger := customlogger.GetInstance()
	config, err := ioutil.ReadFile("./wtvConfig.yaml")
	if err != nil {
		logger.Fatalln("解析配置yaml文件出错 err=", err)
	}
	var setting Wtv
	yaml.Unmarshal(config, &setting)
	return &setting
}

func syncDB()  {
	logger := customlogger.GetInstance()
	setting := parseYaml()
	db, err := sql.Open("sqlite3", setting.DB)
	defer db.Close()
	if err != nil {
		logger.Fatalln("数据库连接失败, err=", err)
	}
	for _, tempId := range setting.TemplateId {
		//获取getChannels接口数据并反序列化
		channels := func() []Channel {
			url := fmt.Sprintf("%v?templateId=%v", setting.GetChannelsUrl, tempId)
			resp, err := http.Get(url)
			if err != nil {
				logger.Fatalln("获取getChannels接口失败 err=", err)
			}
			defer resp.Body.Close()
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				logger.Fatalln("读取getChannels接口内容失败 err=", err)
			}
			if len(b) == 0 {
                logger.Fatalf("模版%s数据为空", tempId)
            }
			var slice []Channel
			err = json.Unmarshal(b, &slice)
			if err != nil {
				logger.Fatalln("getChannels接口反序列化失败 err=", err)
			}
			return slice
		}()
		//创建模版表
		logger.Printf("创建数据表w%v", tempId)
		sqlTemp := fmt.Sprintf("CREATE TABLE w%s(id integer primary key autoincrement, uuid char(50) not null unique, no int not null);", tempId)
		db.Exec(sqlTemp)

		for _, channel := range channels {
			offset := 0
			for _, uuid := range setting.IgnoreUuid {
				if channel.Uuid == uuid {
					logger.Printf("忽略写频道=%s", uuid)
					continue
				} else {
					offset += 1
				}
			}
			if offset == len(setting.IgnoreUuid) {
				func () {
					//模版表里面插入uuid数据
					logger.Printf("表w%s插入%s", tempId, channel.Uuid)
					sqlUuid := fmt.Sprintf("INSERT INTO w%s(uuid, no) values(?, ?);", tempId)
					stmt, err := db.Prepare(sqlUuid)
					if err != nil {
						logger.Fatalln("prepare err=", err)
					}
					_, err = stmt.Exec(channel.Uuid, channel.No)
					if err != nil {
						logger.Fatalln("insert err=", err)
					}
				}()
			}
		}
		logger.Printf("表%s数据插入完成", tempId)
	}
}

func init() {
	_, err := os.Stat("./wtv.db")
	if err == nil {
		os.Remove("./wtv.db")
	}
}

func main() {
	syncDB()
}

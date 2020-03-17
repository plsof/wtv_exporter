package main

import (
	"context"
	"flag"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"time"
)

var cliChoice = flag.String("c", "get", "select your choice \"get/put/del\"")

type Wtv struct {
	TemplateId []string `yaml:"templateid"`
	IgnoreUuid []string `yaml:"ignore_uuid"`
}

//解析配置文件
func parseYaml() *Wtv {
	config, err := ioutil.ReadFile("./wtvConfig.yaml")
	if err != nil {
		log.Fatalln("解析配置yaml文件出错 err=", err)
	}
	var setting Wtv
	yaml.Unmarshal(config, &setting)
	return &setting
}

// 写入数据
func putEtcd() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalln("connect failed, err=", err)
	}
	log.Println("connect successful")
	defer cli.Close()
	setting := parseYaml()
	for k, v := range setting.TemplateId {
		id := fmt.Sprintf("/wtv/monitor/templates/%v", k)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = cli.Put(ctx, id, v)
		cancel()
		if err != nil {
			log.Fatalln("put templateid failed, err=", err)
		}
	}
	log.Println("templateId写入成功")
	for k, v := range setting.IgnoreUuid {
		id := fmt.Sprintf("/wtv/monitor/ignore-uuid/%v", k)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = cli.Put(ctx, id, v)
		cancel()
		if err != nil {
			log.Fatalln("put ignore-uuid failed, err=", err)
		}
	}
	log.Println("ignore-uuid写入成功")
}

// 读取数据
func getEtcd() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalln("connect failed, err=", err)
	}
	log.Println("connect successful")
	defer cli.Close()
	tempResp, tempErr := cli.Get(context.TODO(), "/wtv/monitor/templates", clientv3.WithPrefix())
	if tempErr != nil {
		log.Fatalln("get failed, err:", err)
	}
	for _, ev := range tempResp.Kvs {
		log.Printf("%s : %s\n", ev.Key, ev.Value)
	}
	log.Println("读取模版信息成功")
	uuidResp, uuidErr := cli.Get(context.TODO(), "/wtv/monitor/ignore-uuid", clientv3.WithPrefix())
	if uuidErr != nil {
		log.Fatalln("get failed, err:", uuidErr)
	}
	for _, ev := range uuidResp.Kvs {
		log.Printf("%s : %s\n", ev.Key, ev.Value)
	}
	log.Println("读取频道信息成功")
}

//删除数据
func delEtcd()  {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalln("connect failed, err=", err)
	}
	log.Println("connect successful")
	defer cli.Close()
	_, tempErr := cli.Delete(context.TODO(), "/wtv/monitor/templates", clientv3.WithPrefix())
	if tempErr != nil {
		log.Fatalln("delete templateid failed, err=", tempErr)
	}
	_, uuidErr := cli.Delete(context.TODO(), "/wtv/monitor/ignore-uuid", clientv3.WithPrefix())
	if tempErr != nil {
		log.Fatalln("connect uuid failed, err=", uuidErr)
	}
	log.Println("删除templateid, uuid成功")
}

func main() {
	flag.Parse()
	if *cliChoice == "get" {
		getEtcd()
	} else if *cliChoice == "put" {
		putEtcd()
	} else if *cliChoice == "del" {
		delEtcd()
	} else {
		fmt.Println("输入-c命令行参数错误")
	}
}
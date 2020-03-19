package customlogger

import (
	"log"
	"os"
	"sync"
)

type logger struct {
	Filename string
	Onfile   os.File
	*log.Logger
}

var logInstance *logger
var once sync.Once

// start loggeando
func GetInstance() *logger {
	once.Do(func() {
		logInstance = createLogger("exportLogger.log")
	})
	return logInstance
}

func createLogger(fname string) *logger {
	file, _ := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)

	return &logger{
		Filename: fname,
		Onfile:   *file,
		Logger:   log.New(file, "wtv monitoring ", log.Ldate|log.Ltime|log.Lshortfile),
	}
}
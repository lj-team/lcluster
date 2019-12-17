package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/lj-team/go-generic/log"
)

type Config struct {
	Log    log.Config `json:"log"`
	Server string     `json:"server"`
	Nodes  []string   `json:"nodes"`
}

var _config *Config

func LoadConfig(filename string) *Config {

	var data []byte
	var err error

	if data, err = ioutil.ReadFile(filename); err != nil {
		fmt.Println("read " + filename + " error")
		os.Exit(1)
	}

	var _con Config

	if err = json.Unmarshal(data, &_con); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	_config = &_con

	return &_con
}

func SetConfig(conf *Config) {
	_config = conf
}

func GetConfig() *Config {
	return _config
}

package connect

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

func LoadNodeList(filename string) []string {
	var data []byte
	var err error

	if data, err = ioutil.ReadFile(filename); err != nil {
		fmt.Println("read " + filename + " error")
		os.Exit(1)
	}

	var nodes []string

	if err = json.Unmarshal(data, &nodes); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return nodes

}

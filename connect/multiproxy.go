package connect

import (
	"encoding/json"
	"io/ioutil"
	"time"

	"github.com/lj-team/go-generic/log"
)

type MultiProxy struct {
	rings map[string]Cluster
}

func MultiProxyFromFile(filename string) *MultiProxy {

	log.Info("init lcluster rings from " + filename)

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Error(err.Error())
		<-time.After(time.Second)
		panic(err)
	}

	var conf map[string][]string = map[string][]string{}

	err = json.Unmarshal(data, &conf)
	if err != nil {
		log.Error(err.Error())
		<-time.After(time.Second)
		panic(err)
	}

	obj := &MultiProxy{rings: map[string]Cluster{}}

	for k, v := range conf {
		obj.rings[k] = NewProxy(v)
	}

	return obj
}

func MultiProxyFromMap(conf map[string][]string) *MultiProxy {

	log.Info("init lcluster rings from map")

	obj := &MultiProxy{rings: map[string]Cluster{}}

	for k, v := range conf {
		obj.rings[k] = NewProxy(v)
	}

	return obj
}

func (mp *MultiProxy) Get(name string) Cluster {

	if p, h := mp.rings[name]; h {
		return p
	}

	return nil
}

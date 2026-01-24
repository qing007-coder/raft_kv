package consensus

import "github.com/spf13/viper"

type NodeConfig struct {
	NodeID string `yaml:"node_id"`
	Peers  []Node `yaml:"peers"`
}

type Node struct {
	ID   string `yaml:"id"`
	Addr string `yaml:"addr"`
}

func NewNodeConfig() *NodeConfig {
	conf := new(NodeConfig)
	viper.SetConfigFile("./config/node.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		panic("config file read error: " + err.Error())
	}
	if err = viper.Unmarshal(conf); err != nil {
		panic("config unmarshal error:" + err.Error())
	}

	return conf
}

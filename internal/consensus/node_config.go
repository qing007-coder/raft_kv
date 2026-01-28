package consensus

import "github.com/spf13/viper"

type NodeConfig struct {
	NodeID  string `mapstructure:"node_id"`
	Addr    string `mapstructure:"addr"`
	DataDir string `mapstructure:"data_dir"`
	Peers   []Node `mapstructure:"peers"`
}

type Node struct {
	ID   string `mapstructure:"id"`
	Addr string `mapstructure:"addr"`
}

func NewNodeConfig(configPath string) *NodeConfig {
	conf := new(NodeConfig)
	viper.SetConfigFile(configPath)
	err := viper.ReadInConfig()
	if err != nil {
		panic("config file read error: " + err.Error())
	}
	if err = viper.Unmarshal(conf); err != nil {
		panic("config unmarshal error:" + err.Error())
	}

	return conf
}

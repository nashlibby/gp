package viper

import (
	"github.com/spf13/viper"
	"testing"
)

// # 简单读取配置文件
func Test_ReadConfig(t *testing.T) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(viper.GetString("app"))
	t.Log(viper.GetBool("debug"))

	t.Log(viper.GetString("db.connection"))
	t.Log(viper.GetString("db.username"))
	t.Log(viper.GetString("db.password"))
	t.Log(viper.GetString("db.database"))
}

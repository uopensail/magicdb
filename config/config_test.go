package config

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestConfig_Init(t *testing.T) {
	AppConfigImp.Init("../conf/dev/config.toml")
	data, _ := json.MarshalIndent(AppConfigImp, " ", " ")
	fmt.Print(string(data))
}

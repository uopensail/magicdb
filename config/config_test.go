package config

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestConfig_Init(t *testing.T) {
	AppConfigInstance.Init("../conf/local/config.toml")
	data, _ := json.MarshalIndent(AppConfigInstance, " ", " ")
	fmt.Print(string(data))
}

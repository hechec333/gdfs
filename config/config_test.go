package config

import "testing"

func TestConfig(t *testing.T) {

	cc := GetClusterConfig()

	t.Log(cc)
}

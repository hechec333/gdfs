package main

import (
	"bytes"
	"gdfs/config"
	"gdfs/internal/common"
	"log"
	"os"
	"runtime"
	"text/template"
)

func GenComposeFile() {

	tmpl, err := template.New("docker-compose.yml.tmpl").ParseFiles("docker-compose.yml.tmpl")

	if err != nil {
		panic(err)
	}
	path := "../docker-compose.yml"
	if common.IsExist(path) {
		os.Remove(path)
	}

	cfg := config.GetClusterConfig()

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, *cfg)

	if err != nil {
		panic(err)
	}

	f, err := os.Create("../docker-compose.yml")
	if err != nil {
		panic(err)
	}

	f.WriteString(buf.String())
}

func GenRunScript() {
	var target string
	log.Println("configure platform ", runtime.GOOS)
	if runtime.GOOS == "windows" {
		target = "run.ps1"
	} else {
		target = "run.sh"
	}

	tmpl, err := template.New(target + ".tmpl").ParseFiles(target + ".tmpl")

	if err != nil {
		panic(err)
	}
	path := "../" + target
	if common.IsExist(path) {
		os.Remove(path)
	}
	config.SetPath("..")
	cfg := config.GetClusterConfig()

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, *cfg)

	if err != nil {
		panic(err)
	}

	f, err := os.Create("../" + target)
	if err != nil {
		panic(err)
	}

	f.WriteString(buf.String())
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("please add arg [compose,script]")
	}
	switch os.Args[1] {
	case "compose":
		GenComposeFile()
	case "script":
		GenRunScript()
	default:
		log.Println("only support arg [compose,script]")
	}
}

package main

import (
	_ "embed"

	"xsonar/pkg/shared"

	"github.com/zeromicro/go-zero/rest"
)

//go:embed docs/swagger.json
var consoleSwaggerDoc []byte

type embeddedSwaggerDocSource struct {
	doc []byte
}

func (s embeddedSwaggerDocSource) ReadSwaggerDoc() ([]byte, error) {
	return s.doc, nil
}

func addSwaggerRoutes(server *rest.Server) {
	shared.AddSwaggerRoutes(server, embeddedSwaggerDocSource{
		doc: consoleSwaggerDoc,
	})
}

package main

import (
	_ "embed"

	"xsonar/pkg/shared"

	"github.com/zeromicro/go-zero/rest"
)

//go:embed docs/swagger.json
var gatewaySwaggerDoc []byte

type gatewaySwaggerDocSource struct {
	doc []byte
}

func (s gatewaySwaggerDocSource) ReadSwaggerDoc() ([]byte, error) {
	return s.doc, nil
}

func addSwaggerRoutes(server *rest.Server) {
	shared.AddSwaggerRoutes(server, gatewaySwaggerDocSource{
		doc: gatewaySwaggerDoc,
	})
}

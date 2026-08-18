package main

import _ "embed"

//go:embed webui.html
var webUIPage string

//go:embed webui.js
var webUIScript string

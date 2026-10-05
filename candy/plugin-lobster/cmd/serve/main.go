// Command serve is the OUT-OF-PROCESS entrypoint for plugin-lobster — the one
// plugin that EXECUTES a charly workflow.
//
// A new plugin's default placement is EXTERNAL: charly connects it by word at
// runtime, host-building ./cmd/serve and speaking go-plugin gRPC over it, with
// zero charly-module changes. That build is why this file must exist: without a
// main package here there is no binary for the host to launch, so neither
// `workflow:lobster` (InvokeProvider) nor `command:lobster` (`charly lobster …`)
// could ever be reached — the capabilities would be declared and dead.
//
// sdk.Main serves the provider and its meta over gRPC, and passes CliMain as the
// in-process CLI so the same binary answers `charly lobster import|export|doctor`
// when invoked directly. The SAME provider compiles INTO charly when listed in
// compiled_plugins; placement is invisible above the registry.
package main

import (
	pluginlobster "github.com/opencharly/plugin-lobster/candy/plugin-lobster"
	"github.com/opencharly/sdk"
)

func main() {
	sdk.Main(pluginlobster.NewProvider(), pluginlobster.NewMeta(), pluginlobster.CliMain)
}

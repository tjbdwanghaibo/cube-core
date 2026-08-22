package nest

import "context"

// Tests exercise dispatch internals through an explicitly constructed engine;
// these names exist only in test builds and are not part of the public API.
var Nest *NestMgr

func InitNest(opts ...NestOption) {
	Nest = NewEngine(opts...)
	if err := Nest.Start(); err != nil {
		panic(err)
	}
}

func StopNest() {
	if Nest != nil {
		_ = Nest.Shutdown(context.Background())
		Nest = nil
	}
}

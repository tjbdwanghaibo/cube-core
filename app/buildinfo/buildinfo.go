package buildinfo

import "fmt"

var (
	Version   = ""
	Commit    = ""
	BuildTime = ""
	Dirty     = ""
)

type BuildInfo struct {
	Version   string
	Commit    string
	BuildTime string
	Dirty     string
}

func Info() BuildInfo {
	return BuildInfo{
		Version:   valueOrDefault(Version, "dev"),
		Commit:    valueOrDefault(Commit, "unknown"),
		BuildTime: valueOrDefault(BuildTime, "unknown"),
		Dirty:     valueOrDefault(Dirty, "unknown"),
	}
}

func VersionString() string {
	return Info().String()
}

func (i BuildInfo) String() string {
	return fmt.Sprintf("%s commit=%s built=%s dirty=%s", i.Version, i.Commit, i.BuildTime, i.Dirty)
}

func valueOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

package doomdb

type Config struct {
	SelectOnly bool `env:"SELECT_ONLY" envDefault:"false"`
}

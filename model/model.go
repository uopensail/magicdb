package model

type DataType int
type StoreType int

const (
	StringListType DataType = iota + 1
	Int64ListType
	Float32ListType
)

const (
	TextType StoreType = iota + 1
	RealType
	IntegerType
)


type Feature struct {
	Column     string            `bson:"column" json:"column" toml:"column"`
	DataType   DataType          `bson:"dtype" json:"dtype" toml:"dtype"`
	StoreType  StoreType         `bson:"stype" json:"stype" toml:"stype"`
	Sep        string            `bson:"sep" json:"sep" toml:"sep"`
}

type Machine struct {
	DataBase string `bson:"database" json:"database" toml:"database"`
}

type DataBase struct {
	Machines []string `bson:"machines" json:"machines" toml:"machines"`
	Name string `bson:"name" json:"name" toml:"name"`
	Bucket string  `bson:"bucket" json:"bucket" toml:"bucket"`
	Tables []string  `bson:"tables" json:"tables" toml:"tables"`
	AccessKey string  `bson:"access_key" json:"access_key" toml:"access_key"`
	SecretKey string  `bson:"secret_key" json:"secret_key" toml:"secret_key"`
}

type Table struct {
	Name string `bson:"name" json:"name" toml:"name"`
	DataBase string  `bson:"database" json:"database" toml:"database"`
	Tables []string  `bson:"tables" json:"tables" toml:"tables"`
	DataDir string  `bson:"data" json:"data" toml:"data"`
	MetaDir string  `bson:"meta" json:"meta" toml:"meta"`
	Versions []string `bson:"versions" json:"versions" toml:"versions"`
	Current string  `bson:"current" json:"current" toml:"current"`
}
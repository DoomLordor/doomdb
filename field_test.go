package doomdb

import (
	"reflect"
	"testing"
	"time"
)

type testFields struct {
	IntField    uint64    `db:"test.int_field" default:"3"`
	StringField string    `db:"test.string_field" default:"testDefault"`
	TimeField   time.Time `db:"test.time_field"`
	UuidField   string    `db:"test.uuid_field" default:"uuid"`
	JoinField   string    `db:"test2.join_field" join:"LEFT JOIN test2 on test2.id=test.join"`
	SkipField   string    `db:"-"`
}

func TestFieldDB(t *testing.T) {

	field := fieldDB{
		name: "test_name",
		join: "",
	}

	if field.toSelect() != `test_name AS "test_name"` {
		t.Error(`select wrong`)
	}

}

func TestFieldDBWithDefault(t *testing.T) {

	field := fieldDB{
		name: "test_name",
		def:  "test_default,test_default1, test_default2",
		join: "",
	}

	if field.toSelect() != `COALESCE(test_name, 'test_default', 'test_default1', 'test_default2') AS "test_name"` {
		t.Error(`select with default wrong`)
	}

}

func TestParseValidate(t *testing.T) {
	ref := reflect.ValueOf(any(0))
	fields := parseFields(ref)
	if fields != nil {
		t.Fatal("work with a non struct type")
	}
}

func TestParseLen(t *testing.T) {
	ref := reflect.ValueOf(testFields{})

	fields := parseFields(ref)

	if len(fields) != ref.NumField()-1 {
		t.Fatal("parse extra fields")
	}
}

func TestParseName(t *testing.T) {

	ref := reflect.ValueOf(&testFields{})

	fields := parseFields(ref)

	names := make([]string, 0, len(fields))

	if fields[0].name != "test.int_field" {
		names = append(names, "int_field")
	}

	if fields[1].name != "test.string_field" {
		names = append(names, "string_field")
	}

	if fields[2].name != "test.time_field" {
		names = append(names, "time_field")
	}

	if fields[3].name != "test.uuid_field" {
		names = append(names, "uuid_field")
	}

	if fields[4].name != "test2.join_field" {
		names = append(names, "join_field")
	}

	for _, name := range names {
		t.Errorf("parse name %s error", name)
	}
}

func TestParseDefault(t *testing.T) {

	ref := reflect.ValueOf(&testFields{})

	fields := parseFields(ref)

	names := make([]string, 0, len(fields))

	if fields[0].def != "3" {
		names = append(names, "int_field")
	}

	if fields[1].def != "testDefault" {
		names = append(names, "string_field")
	}

	if fields[2].def != "" {
		names = append(names, "time_field")
	}

	if fields[3].def != DefUUID {
		names = append(names, "uuid_field")
	}

	if fields[4].def != "" {
		names = append(names, "join_field")
	}

	for _, name := range names {
		t.Errorf("parse default %s error", name)
	}
}

func TestParseJoin(t *testing.T) {

	ref := reflect.ValueOf(&testFields{})

	fields := parseFields(ref)

	names := make([]string, 0, len(fields))

	if fields[0].join != "" {
		names = append(names, "int_field")
	}

	if fields[1].join != "" {
		names = append(names, "string_field")
	}

	if fields[2].join != "" {
		names = append(names, "time_field")
	}

	if fields[3].join != "" {
		names = append(names, "uuid_field")
	}

	if fields[4].join != "LEFT JOIN test2 on test2.id=test.join" {
		names = append(names, "join_field")
	}

	for _, name := range names {
		t.Errorf("parse join %s error", name)
	}
}

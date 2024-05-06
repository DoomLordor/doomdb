package doomdb

import (
	"fmt"
	"reflect"
	"strings"
)

const (
	TagDB        = "db"
	TagDBDefault = "default"
	TagSkip      = "-"
	TagJoin      = "join"
	TagEmpty     = ""

	DefKeyUUID = "uuid"
	DefUUID    = "00000000-0000-0000-0000-000000000000"
)

type fieldDB struct {
	name string
	def  string
	join string
}

func (f *fieldDB) toSelect() string {
	if f.def == "" {
		return fmt.Sprintf(`%s AS "%s"`, f.name, f.name)
	}
	return f.toSelectWithDefault()
}

func (f *fieldDB) toSelectWithDefault() string {

	values := strings.Split(f.def, ",")
	for i, value := range values {
		value = strings.Trim(value, " ")
		values[i] = fmt.Sprintf("'%s'", value)
	}
	def := strings.Join(values, ", ")
	return fmt.Sprintf(`COALESCE(%s, %s) AS "%s"`, f.name, def, f.name)
}

func (f *fieldDB) toInsertName() string {
	return fmt.Sprintf(`"%s"`, f.name)
}

func (f *fieldDB) toInsert() string {
	return ":" + f.name
}

func (f *fieldDB) toUpdate() string {
	return fmt.Sprintf(`"%s"=:%s`, f.name, f.name)
}

func parseFields(t reflect.Type) []*fieldDB {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil
	}

	n := t.NumField()
	fields := make([]*fieldDB, 0, n)
	for i := 0; i < n; i++ {
		field := t.Field(i)
		fieldName := field.Tag.Get(TagDB)
		if fieldName == TagSkip || fieldName == TagEmpty {
			continue
		}

		st := field.Type
		if st.Kind() == reflect.Pointer {
			st = st.Elem()
		}

		if fieldName == "" && st.Kind() == reflect.Struct {
			fields = append(fields, parseFields(st)...)
			continue
		}

		def := field.Tag.Get(TagDBDefault)

		if def == DefKeyUUID {
			def = DefUUID
		}

		fieldData := &fieldDB{
			name: fieldName,
			def:  def,
			join: field.Tag.Get(TagJoin),
		}
		fields = append(fields, fieldData)
	}

	return fields
}

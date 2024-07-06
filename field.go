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

	DefKeyUUID = "uuid"
	DefUUID    = "00000000-0000-0000-0000-000000000000"
)

type fieldDB struct {
	name  string
	def   string
	join  string
	value any
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

func parseFields(v reflect.Value) []*fieldDB {
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil
	}

	n := v.NumField()
	t := v.Type()
	fields := make([]*fieldDB, 0, n)
	for i := 0; i < n; i++ {
		structField := t.Field(i)

		fieldName := structField.Tag.Get(TagDB)
		if fieldName == TagSkip {
			continue
		}

		valueField := v.Field(i)

		if valueField.Kind() == reflect.Pointer {
			valueField = valueField.Elem()
		}

		if fieldName == "" && valueField.Kind() == reflect.Struct {
			fields = append(fields, parseFields(valueField)...)
			continue
		}

		def := structField.Tag.Get(TagDBDefault)

		if def == DefKeyUUID {
			def = DefUUID
		}

		fieldData := &fieldDB{
			name:  fieldName,
			def:   def,
			join:  structField.Tag.Get(TagJoin),
			value: v.Field(i).Interface(),
		}
		fields = append(fields, fieldData)
	}

	return fields
}

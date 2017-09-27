package elasticsearchv5

import (
	"log"
	"testing"

	"git.raceresult.com/vendor/github.com/stretchr/testify/assert"
)

var indicesTests = []struct {
	index    string
	expected bool
}{
	{"unit_1", true},
	{"unit_2", true},
	{"unit_3", true},
}

func TestIndexExists(t *testing.T) {
	url = []string{"http://127.0.0.1:9200"}
	el, err := New("unit_test", "", "")
	if err != nil {
		t.Error(err)
	}
	for _, tt := range indicesTests {
		if exists, err := el.IndexExists(tt.index); err != nil {
			log.Print("err")
			t.Error(err)
		} else if exists {
			log.Print("came here")
			if err := el.DeleteIndex(tt.index); err != nil {
				t.Error(err)
			}
		}
	}
}

func TestCreateIndex(t *testing.T) {
	el, err := New("unit_test", "", "")
	if err != nil {
		t.Error(err)
	}
	for _, tt := range indicesTests {
		if err := el.CreateIndex(tt.index, ""); err != nil {
			t.Error(err)
		}
	}
}

func TestDeleteIndex(t *testing.T) {
	el, err := New("unit_test", "", "")
	if err != nil {
		t.Error(err)
	}
	for _, tt := range indicesTests {
		if err := el.DeleteIndex(tt.index); err != nil {
			t.Error(err)
		}
	}
}

var indexingTests = []struct {
	doc      interface{}
	id       string
	expected string
}{
	{`{"test": "bla"}`, "1", "1"},
	{`{"test": "blubb"}`, "2", "2"},
	{`{"test": "bling"}`, "asf2", "asf2"},
}

func TestIndex(t *testing.T) {
	el, err := New("unit_test", "test", "")
	if err != nil {
		t.Error(err)
	}
	for _, tt := range indexingTests {
		if actual, err := el.Index(tt.doc, tt.id); err != nil {
			t.Error(err)
		} else if actual != tt.expected {
			t.Errorf("Fib(%s): expected %s, actual %s", tt.doc, tt.expected, actual)
		}
	}
}

var getTests = []struct {
	doc map[string]string
	id  string
}{
	{map[string]string{"test": "bla"}, "1"},
	{map[string]string{"test": "blubb"}, "2"},
	{map[string]string{"test": "bling"}, "asf2"},
}

func TestGet(t *testing.T) {
	el, err := New("unit_test", "test", "")
	if err != nil {
		t.Error(err)
	}
	for _, tt := range getTests {
		actual := map[string]string{}
		if err := el.Get(tt.id, &actual); err != nil {
			t.Error(err)
		}
		assert.Equal(t, tt.doc, actual)
	}
}

var searchTests = []struct {
	json     string
	expected int64
}{
	{`{
		"query": {
			"bool": {
				"must": {
					"match": {"test": "bla"}
				}
			}
		}
	}`, 1},
}

func TestSearch(t *testing.T) {
	el, err := New("unit_test", "test", "")
	if err != nil {
		t.Error(err)
	}
	for _, tt := range searchTests {
		if actual, err := el.Search(tt.json); err != nil {
			t.Error(err)
		} else if actual.TotalHits() != tt.expected {
			t.Error(actual, tt.expected)
		}
	}
}

var deleteTests = []struct {
	id       string
	expected bool
}{
	{"2", true},
	{"asf2", true},
}

func TestDelete(t *testing.T) {
	el, err := New("unit_test", "test", "")
	if err != nil {
		t.Error(err)
	}
	for _, tt := range deleteTests {
		if found, err := el.Delete(tt.id); err != nil {
			t.Error(err)
		} else if found != tt.expected {
			t.Error(found, tt.expected)
		}
	}
}

var indexTemplateTests = []struct {
	name         string
	templateBody string
}{
	{"templ1", `{
		"template": "te*",
		"settings" : {
			"number_of_shards" : 1
		},
		"mappings" : {
			"type1" : {
				"_source" : { "enabled" : false }
			}
		}
	}`},
}

func TestPutIndexTemplate(t *testing.T) {
	el, err := New("unit_test", "test", "")
	if err != nil {
		t.Error(err)
	}
	for _, tt := range indexTemplateTests {
		if err := el.PutIndexTemplate(tt.name, tt.templateBody); err != nil {
			t.Error(err)
		}
	}
}

func TestDeleteIndexTemplate(t *testing.T) {
	el, err := New("unit_test", "test", "")
	if err != nil {
		t.Error(err)
	}
	for _, tt := range indexTemplateTests {
		if err := el.DeleteIndexTemplate(tt.name); err != nil {
			t.Error(err)
		}
	}
}

// func Test1(t *testing.T) {
// 	conn.PutIndexTemplate("rrmail_template", `{
// 		"template" : "rrmail-*",
// 		"settings" : {
// 			"analysis": {
// 				"analyzer": {
// 					"html_analyzer": {
// 						"tokenizer":     "standard",
// 						"char_filter": [ "html_strip" ]
// 					}
// 				}
// 			}
// 		},
// 		"mappings" : {
// 			"mail": {
// 				"properties": {
// 					"attachmentCount": {"type": "integer"},
// 					"bcc": {"type": "string"},
// 					"cc": {"type": "string"},
// 					"contentType": {"type": "string", "index" : "not_analyzed"},
// 					"flaghashcode": {"type": "long"},
// 					"flags": {"type": "string", "index" : "not_analyzed"},
// 					"folderFullName": {"type": "string", "index" : "not_analyzed"},
// 					"folderUri": {"type": "string", "index" : "not_analyzed"},
// 					"from.email": {"type": "string"},
// 					"from.personal": {"type": "string"},
// 					"htmlContent": {"type": "string", "index" : "analyzed", "analyzer" : "html_analyzer"},
// 					"mailboxType": {"type": "string", "index" : "not_analyzed"},
// 					"receivedDate": {"type": "date", "ignore_malformed": "true"},
// 					"sendDate": {"type": "date", "ignore_malformed": "true"},
// 					"size": {"type": "long"},
// 					"subject": {"type": "string"},
// 					"textContent": {"type": "string"},
// 					"to": {"type": "object"},
// 					"uid": {"type": "long"}
// 				}
// 			}
// 		}
// 	}`)
// }

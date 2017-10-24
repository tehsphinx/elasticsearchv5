package elasticsearchv5

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"

	elasticv5 "gopkg.in/olivere/elastic.v5"
)

// Elastic interface handles ElasticSearch connections. Manages connection internally.
type Elastic struct {
	url               []string
	index             string
	docType           string
	mapping           string
	bulk              *elasticv5.BulkService
	bulkSize          int
	ctx               context.Context
	sequence          *sequence
	sequenceCacheSize int
}

var client *elasticv5.Client
var url = []string{"http://127.0.0.1:9200"}

// New creates a new Elastic client. All elastic clients use the same connection.
func New(index string, docType string, mapping string, elasticURLs ...string) (*Elastic, error) {
	es := &Elastic{
		url:     url,
		index:   index,
		docType: docType,
		mapping: mapping,
		ctx:     context.Background(),
	}
	err := es.checkClient(true, elasticURLs)
	return es, err
}

// SetSequenceAutoIncrement sets the index to use integer auto increment ids
func (es *Elastic) SetSequenceAutoIncrement(cacheSize int) error {
	es.sequenceCacheSize = cacheSize
	if es.sequenceCacheSize == 0 {
		es.sequenceCacheSize = 1
	}

	var err error
	es.sequence, err = newSequence(es)
	return err
}

// StartBulk starts bulk indexint with documents per bulk
func (es *Elastic) StartBulk(size int) {
	es.bulk = client.Bulk()
	es.bulkSize = size
}

// StopBulk stops bulk indexing and makes sure all remaining documents are being sent
func (es *Elastic) StopBulk() error {
	_, err := es.bulk.Do(es.ctx)
	es.bulk = nil
	return err
}

// BulkIndex indexes a list of documents. Only works without id
// NOT to be used with StartBulk / StopBulk
func (es *Elastic) BulkIndex(docs []interface{}) (*elasticv5.BulkResponse, error) {
	b := client.Bulk().Index(es.index).Type(es.docType)
	for _, doc := range docs {
		q := elasticv5.NewBulkIndexRequest().Doc(doc)
		b.Add(q)
	}
	return client.Bulk().Do(es.ctx)
}

// Index creates a document in elasticsearch
func (es *Elastic) Index(doc interface{}, id string) (string, error) {
	if es.sequence != nil && id == "" {
		id = es.sequence.GetID()
	}
	if es.bulk != nil {
		return "", es.bulkIndex(doc, id)
	}

	q := client.Index().Index(es.index).Type(es.docType).BodyJson(doc)
	if id != "" {
		q = q.Id(id)
	}

	res, err := q.Do(es.ctx)
	if err != nil {
		return "", err
	}
	return res.Id, nil
}

func (es *Elastic) bulkIndex(doc interface{}, id string) error {
	q := elasticv5.NewBulkIndexRequest().Index(es.index).Type(es.docType).Doc(doc)
	if id != "" {
		q = q.Id(id)
	}
	es.bulk.Add(q)

	return es.execBulk()
}

// Update updates a existing document with given doc
func (es *Elastic) Update(doc interface{}, id string) error {
	if id == "" {
		return errors.New("update needs an id")
	}
	if es.bulk != nil {
		return es.bulkUpdate(doc, id)
	}

	_, err := client.Update().Index(es.index).Type(es.docType).Doc(doc).Id(id).Do(es.ctx)
	return err
}

func (es *Elastic) bulkUpdate(doc interface{}, id string) error {
	q := elasticv5.NewBulkUpdateRequest().Index(es.index).Type(es.docType).Doc(doc).Id(id)
	es.bulk.Add(q)

	return es.execBulk()
}

func (es *Elastic) execBulk() error {
	if es.bulk.NumberOfActions() >= es.bulkSize {
		if _, err := es.bulk.Do(es.ctx); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves a document from elasticsearch by id
func (es *Elastic) Get(id string, doc interface{}) error {
	res, err := client.Get().Index(es.index).Type(es.docType).Id(id).Do(es.ctx)
	if err != nil {
		return err
	}

	return json.Unmarshal(*res.Source, doc)
}

// GetMulti gets multiple elements from elasticsearch by id
func (es *Elastic) GetMulti(ids ...string) (*elasticv5.MgetResponse, error) {
	g := client.Mget()
	for _, id := range ids {
		item := elasticv5.NewMultiGetItem().Index(es.index).Type(es.docType).Id(id)
		g.Add(item)
	}
	return g.Do(es.ctx)
}

// Delete removes one document from elasticsearch by id
func (es *Elastic) Delete(id string) (bool, error) {
	res, err := client.Delete().Index(es.index).Type(es.docType).Id(id).Do(es.ctx)
	if err != nil {
		return false, err
	}
	return res.Found, nil
}

// Search takes a json search string and executes it, returning the result
func (es *Elastic) Search(json interface{}) (*elasticv5.SearchResult, error) {
	return client.Search(es.index).Type(es.docType).Source(json).Pretty(true).Do(es.ctx)
}

// IndexExists checks if index exists
func (es *Elastic) IndexExists(index string) (bool, error) {
	return client.IndexExists(index).Do(es.ctx)
}

// CreateIndex creates a index by name. The index specified in the struct is created anyway if it doesnt exist.
func (es *Elastic) CreateIndex(index, body string) error {
	createIndex, err := client.CreateIndex(index).Body(body).Do(es.ctx)
	if err == nil && !createIndex.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge new index")
	}
	return err
}

// DeleteIndex deletes the index specified in the struct.
func (es *Elastic) DeleteIndex(index string) error {
	deleteIndex, err := client.DeleteIndex(index).Do(es.ctx)
	if err == nil && !deleteIndex.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge deletion of index")
	}
	return err
}

func (es *Elastic) createIndex() error {
	err := es.CreateIndex(es.index, "")
	return err
}

// PutIndexTemplate crates a index template
func (es *Elastic) PutIndexTemplate(name string, body string) error {
	res, err := client.IndexPutTemplate(name).BodyString(body).Do(es.ctx)
	if err == nil && !res.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge creation of template")
	}
	return err
}

// DeleteIndexTemplate deletes a index template
func (es *Elastic) DeleteIndexTemplate(name string) error {
	res, err := client.IndexDeleteTemplate(name).Do(es.ctx)
	if err == nil && !res.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge deletion of template")
	}
	return err
}

// GetMapping retrieves the mapping of the current doctype
func (es *Elastic) GetMapping() (map[string]interface{}, error) {
	return client.GetMapping().Index(es.index).Type(es.docType).Do(es.ctx)
}

// PutMapping sets the mapping for the current doctype
func (es *Elastic) PutMapping() error {
	res, err := client.PutMapping().Index(es.index).Type(es.docType).
		BodyString(es.mapping).Do(es.ctx)
	if err == nil && !res.Acknowledged {
		err = errors.New("elasticsearch did not acklowledge creation of mapping")
	}
	return err
}

// Aggregate executes aggregation(s) on the server
func (es *Elastic) Aggregate(json interface{}) (*elasticv5.Aggregations, error) {
	res, err := client.Search().Index(es.index).Type(es.docType).Source(json).
		Pretty(true).Do(es.ctx)
	if err != nil {
		return nil, err
	}
	return &res.Aggregations, err
}

func (es *Elastic) checkClient(checkIndex bool, elasticURLs []string) error {
	var err error
	if client == nil {
		err = es.newClient(elasticURLs)
		if err != nil {
			log.Fatal(err)
		} else if checkIndex {
			es.checkOwnIndex()
			es.checkOwnMapping()
		}
	}
	return err
}

func (es *Elastic) checkOwnIndex() error {
	exists, err := client.IndexExists(es.index).Do(es.ctx)
	if err == nil && !exists {
		err = es.createIndex()
	}
	if err != nil {
		log.Fatal(err)
	}
	return err
}

func (es *Elastic) checkOwnMapping() error {
	return nil
}

func (es *Elastic) newClient(elasticURLs []string) error {
	// Moved this here, so this only gets set if there is a connect happening.
	// Otherwise the paramater elasticURLs is ignored anyway cause there is only
	// 'connection' to elasticsearch.
	if len(elasticURLs) != 0 {
		url = elasticURLs
		// Update url in the struct in case it changed
		es.url = url
	}

	log.Println("Opening new Elastic connection to", url)
	cl, err := elasticv5.NewSimpleClient(elasticv5.SetURL(url...),
		elasticv5.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
		elasticv5.SetInfoLog(log.New(ioutil.Discard, "", log.LstdFlags)),
		// elastic.SetInfoLog(log.New(os.Stderr, "ELASTIC Info ", log.LstdFlags)),
		// elastic.SetTraceLog(log.New(os.Stderr, "ELASTIC Trace ", log.LstdFlags)),
		elasticv5.SetBasicAuth("elastic", "changeme"))
	if err == nil {
		client = cl
	}
	return err
}
